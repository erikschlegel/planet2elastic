# pbftoelastic
Pushes a set of OpenStreetMap planet files to a Tile Based Elastic Index

##Setup
Please complete the following steps before you kickoff the elastic indexing. This assumes you have some general familiarity with elasticsearch.

###Environment Settings
Setup the following environment variables to specify the destination elastic cluster. This assumes your elastic cluster has basic authentication enabled.
+ ES_HOST: my-elasticbeast.azure.cloud.com
+ ES_PORT: 9200
+ ES_AUTH_USER: my-username
+ ES_AUTH_PWD: mypassword

Provide the directory location for your planet files  
+ PBF_DIRECTORY: /pbfsource
 
###Setup Elastic Indexes
Create two new indexes. To create a new index in elastic you provide a PUT request using a tool like [POSTMON](https://www.getpostman.com/) with the request URL following the convention ES_HOST:ES_PORT/ES_INDEX_NAME
+ places: [places index defintion](https://github.com/erikschlegel/pbftoelastic/blob/master/indexes/places.json)
+ tiles: [tiles index definition](https://github.com/erikschlegel/pbftoelastic/blob/master/indexes/tiles.json)
