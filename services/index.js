"use strict"

import pbf2json from 'pbf2json';
import through from 'through2';
import elasticService from './elasticIndexer'
import Quadkey from 'tilebelt';
import glob from 'glob';
import turf from 'turf';
import geoTools from 'wgs84-intersect-util';

const FEATURE_TAGS = [
  'amenity',
  'building',
  'historic',
  'name',
  'cuisine',
  'public_transport',
  'tourism'
];

let processedTiles = new Set();
let tileGeometryPolygon = new Map();

function markIndexedTilesAsCompleted(tileMap){
    for(let [key, value] of tileMap){
        processedTiles.add(key)
    }
}

function handleSinglePointTileWays(wayMapFeatures, wayCountMap){
    var count = 0;
    for(let [key, feature] of wayCountMap){
        if(feature.coordinateCount && feature.coordinateCount == 1){
            let lineFeature = turf.lineString([feature.fromCoord, feature.toCoord]);
            let wayFeature = wayMapFeatures.get(key);
            let tileBboxPoly = tileGeometryPolygon.get(feature.quadKey);
            let tileEdgePoint = geoTools.intersectLineBBox(lineFeature, tileBboxPoly);

            if(tileEdgePoint && tileEdgePoint.length > 0 && tileEdgePoint[0].coordinates){
                let edgeCoordinatePoint = tileEdgePoint[0].coordinates;
                if(wayFeature){
                      wayFeature.coordinates.coordinates[0][1] = new Array();
                      wayFeature.coordinates.coordinates[0][1] = edgeCoordinatePoint;
                }
            }
            
            if(wayFeature.coordinates.coordinates[0].length == 1){
                console.log(JSON.stringify(tileEdgePoint));
                console.log('Formatted:');
                console.log(JSON.stringify(wayFeature));
                console.log("tileEdgePoint is either null or not a true point.");
                process.exit();
            }
        }
    }
}

function captureTileData(tileMap, quadKey){
    if(!tileMap.has(quadKey)){
           tileMap.set(quadKey, {id: quadKey, quadKey: quadKey});
           let bbox = Quadkey.tileToBBOX(Quadkey.quadkeyToTile(quadKey));
           tileGeometryPolygon.set(quadKey, bbox);
    }
}

function processPbfFile(filename){
    var batchSize = 100;
    let count= 0;
    let tilesToIndex = new Map();
    let placesToIndex = new Map();
    let config = {
        tags: FEATURE_TAGS,
        leveldb: './tmp',
        file: process.env.PBF_DIRECTORY + '/' + filename
    };

    let connection = elasticService.elasticConnection();

    pbf2json.createReadStream(config).pipe(
        through.obj( (item, e, next) => {
          let feature = {
              osmId: item.id.toString(),
              type: item.type
          };

          FEATURE_TAGS.forEach(featureTag => {
              if (!item.tags[featureTag]) return;

              feature[featureTag] = item.tags[featureTag];
          });

          if (item.type === 'node') {
              feature.location = {
                  lat: Number(item.lat),
                  lon: Number(item.lon)
              };

              feature.quadKey = Quadkey.tileToQuadkey(Quadkey.pointToTile(Number(feature.location.lng), Number(feature.location.lat), 16));
              feature.id = feature.quadKey + '-' + feature.osmId;
              placesToIndex.set(feature.quadKey + "-" + feature.osmId, feature);
              captureTileData(tilesToIndex, feature.quadKey);
          }else if (item.type === 'way' && item.nodes.length > 1) {
              feature.coordinates = {
                   type: "multilinestring",
                   coordinates: new Array()
              };

              if(item.centroid){
                  feature.location = {
                    lat: Number(item.centroid.lat),
                    lon: Number(item.centroid.lon)
                  };
              }

              feature.coordinates.coordinates[0] = new Array();
              let wayMap = new Map();
              
              item.nodes.map((node, index) => {
                  let quadKey = Quadkey.tileToQuadkey(Quadkey.pointToTile(Number(node.lon), Number(node.lat), 16));
                  let mapFeature = placesToIndex.get(quadKey + "-" + feature.osmId);                  
                  let coordCurLength = 0;
                  
                  if(mapFeature){
                     coordCurLength = mapFeature.coordinates.coordinates[0].length;
                  }else{
                     mapFeature = JSON.parse(JSON.stringify(feature));
                  }

                  let key = quadKey + '-' + mapFeature.osmId;
                  let wayMapFeature = wayMap.get(key);
                  //We need to simulate the destination waypoint as the coordinates geoshape is a multilinestring
                  //which requires the coordinate count to be >= 2.
                  let simulatedLineStringPoint = (index == (item.nodes.length - 1))?item.nodes[index - 1]:item.nodes[index + 1];
                  if(!wayMapFeature){
                      wayMapFeature = {
                          quadKey: quadKey,
                          coordinateCount: 1, fromCoord: [Number(node.lon), Number(node.lat)], 
                          toCoord: [Number(simulatedLineStringPoint.lon), Number(simulatedLineStringPoint.lat)]
                      };
                  }else{
                      wayMapFeature.coordinateCount += 1;
                  }

                  wayMap.set(key, wayMapFeature);

                  mapFeature.quadKey = quadKey;
                  mapFeature.id = key;
                  mapFeature.coordinates.coordinates[0][coordCurLength] = new Array();
                  mapFeature.coordinates.coordinates[0][coordCurLength] = [Number(node.lon), Number(node.lat)];
                  placesToIndex.set(key, mapFeature);
                  captureTileData(tilesToIndex, quadKey);
              });

              handleSinglePointTileWays(placesToIndex, wayMap);
          }
          
          count += 1;
          if (count % 100 === 0)
                 console.log('count: ' + count);
          
          if(placesToIndex.size >= batchSize){
              elasticService.elasticBulkIndex(connection, new Map(tilesToIndex), new Map(placesToIndex),() => {
                  markIndexedTilesAsCompleted(tilesToIndex);
                  tilesToIndex.clear();
                  placesToIndex.clear();
                  next();
              });
          }else{
              next();
          } 
        })
     )
}

function elasticUpsert(){
    console.log('services inited');
    
    glob("**/*.pbf", {cwd: process.env.PBF_DIRECTORY}, (er, files) => {
        files.map(file => {
            console.log('Processing ' + file);
            processPbfFile(file);
        });
    });
}


module.exports = {
    elasticUpsert: elasticUpsert
}