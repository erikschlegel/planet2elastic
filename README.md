# Planet 2 Elastic

[![Join the chat at https://gitter.im/erikschlegel/planet2elastic](https://badges.gitter.im/erikschlegel/planet2elastic.svg)](https://gitter.im/erikschlegel/planet2elastic?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Pushes a set of OpenStreetMap planet files to an Tile Based Elastic Index. You can grab planet files at [GeoFabrik](http://download.geofabrik.de/). This will parse all binary pbf files and index all nodes and ways within their respective tile quadkey location. The end result is an elastic document that looks like the following. Document index convention follows {QuadKey}-{OsmId}
```
{
        "_index": "places",
        "_type": "place",
        "_id": "1202033221320231-100149001",
        "_score": 1,
        "_source": {
          "osmId": "100149001",
          "type": "way",
          "name": "FC Berwangen",
          "coordinates": {
            "type": "multilinestring",
            "coordinates": [
              [
                [
                  8.98524,
                  49.186888
                ],
                [
                  8.98611,
                  49.186606
                ],
                [
                  8.985465,
                  49.185754
                ],
                [
                  8.984595,
                  49.186035
                ],
                [
                  8.98524,
                  49.186888
                ]
              ]
            ]
          },
          "location": {
            "lat": 49.186434,
            "lon": 8.98533
          },
          "quadKey": "1202033221320231",
          "id": "1202033221320231-100149001"
        }
      }
```

##Setup
Please complete the following steps before you kickoff the elastic indexing. This assumes you have some general familiarity with elasticsearch. Please refer to this [Gist](https://gist.github.com/erikschlegel/0f4330009c7c5ae83831889609a8bb7c) if you have an Azure subscription and in need of a cloud-based elastic cluster.

###Environment Settings
Setup the following environment variables to specify the destination elastic cluster. This assumes your elastic cluster has basic authentication enabled, via [Shield](https://www.elastic.co/guide/en/shield/current/enable-basic-auth.html).
+ ES_HOST: my-elasticbeast.azure.cloud.com
+ ES_PORT: 9200
+ ES_AUTH_USER: my-username
+ ES_AUTH_PWD: mypassword

Provide the directory location for your planet(.pbf) files  
+ PBF_DIRECTORY: /pbfsource
 
###Setup Elastic Indexes
Create two new indexes. To create a new index in elastic you provide a PUT request using a tool like [POSTMON](https://www.getpostman.com/) with the request URL following the convention ES_HOST:ES_PORT/ES_INDEX_NAME
+ places: [places index defintion](https://github.com/erikschlegel/pbftoelastic/blob/master/indexes/places.json)
+ tiles: [tiles index definition](https://github.com/erikschlegel/pbftoelastic/blob/master/indexes/tiles.json)

##Import
###Configuration
The mapping features that this framework imports from Openstreetmap is managed in [featureTags.js](https://github.com/erikschlegel/Planet2Elastic/blob/master/featureTags.js). Feel free to add/remove the data attributes relevant for your scenario(s).

###Run Importer
```
npm install
npm run import
```
