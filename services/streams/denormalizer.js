"use strict"

import through from 'through2';
import elasticService from '../elasticIndexer'
import Quadkey from 'tilebelt';
import turf from 'turf';
import geoTools from 'wgs84-intersect-util';
import featureTags from '../../featureTags';
import coordDuplicateCheck from '../duplicate_coord_check';

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
        let wayFeature = wayMapFeatures.get(key);

        if(feature.coordinateCount && feature.coordinateCount == 1){
            let lineFeature = turf.lineString([feature.fromCoord, feature.toCoord]);
            let tileBboxPoly = tileGeometryPolygon.get(feature.quadKey);
            let tileEdgePoint = geoTools.intersectLineBBox(lineFeature, tileBboxPoly);

            if(tileEdgePoint && tileEdgePoint.length > 0 && tileEdgePoint[0].coordinates){
                let edgeCoordinatePoint = tileEdgePoint[0].coordinates;
                if(wayFeature){
                      wayFeature.coordinates.coordinates[0][1] = new Array();
                      wayFeature.coordinates.coordinates[0][1] = edgeCoordinatePoint;
                      wayFeature["simulated"] = true;
                }
            }
            
            if(wayFeature.coordinates.coordinates[0].length == 1){
                console.log(JSON.stringify(tileEdgePoint));
                console.log('Formatted:');
                console.log(JSON.stringify(wayFeature));
                console.log("tileEdgePoint is either null or not a true point.");
                process.exit();
            }else if(feature.coordinateCount == 2 && wayFeature.location
                 && coordDuplicateCheck.duplicateExists(wayFeature.coordinates.coordinates)){
                delete wayFeature["coordinates"];
                wayFeature.type = 'node';
            }
        //If a way has only two coordinates which are both the same then convert into a single point node.
        }else if(feature.coordinateCount && feature.coordinateCount == 2
                 && wayFeature.location && coordDuplicateCheck.duplicateExists(wayFeature.coordinates.coordinates[0])){
                delete wayFeature["coordinates"];
                wayFeature.type = 'node';
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

function denormalizer(connection){
  let batchSize = 100;
  let count= 0;
  let tilesToIndex = new Map();
  let placesToIndex = new Map();

  return through.obj(( item, enc, next ) => {
          let feature = {
              osmId: item.id.toString(),
              type: item.type
          };

          featureTags.FEATURE_TAGS.forEach(featureTag => {
              if (!item.tags[featureTag]) return;

              feature[featureTag] = item.tags[featureTag];
          });

          if (item.type === 'node') {
              feature = denormalizeNode(feature, item, placesToIndex, tilesToIndex);
          }else if (item.type === 'way' && item.nodes.length > 1) {
              feature = denormalizeWay(feature, item, placesToIndex, tilesToIndex);
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
  });
}

function denormalizeNode(feature, item, placesToIndex, tilesToIndex){
    feature.location = {
            lat: Number(item.lat),
            lon: Number(item.lon)
    };

    feature.quadKey = Quadkey.tileToQuadkey(Quadkey.pointToTile(Number(feature.location.lng), Number(feature.location.lat), 16));
    feature.id = feature.quadKey + '-' + feature.osmId;
    placesToIndex.set(feature.quadKey + "-" + feature.osmId, feature);
    
    captureTileData(tilesToIndex, feature.quadKey);

    return feature;
}

function denormalizeWay(feature, item, placesToIndex, tilesToIndex){
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
              
    item.nodes.map((node, index) => denormalizeWayNodePoint(item, node, wayMap, 
                                                            placesToIndex, feature, 
                                                            index, tilesToIndex));

    handleSinglePointTileWays(placesToIndex, wayMap);

    return feature;
}

function denormalizeWayNodePoint(item, node, wayMap, placesToIndex, feature, index, tilesToIndex){
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
}

module.exports = {
    stream: denormalizer
}