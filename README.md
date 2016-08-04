# pbftoelastic
Pushes a set of OpenStreetMap planet files to a Tile Based Elastic Index

##Setup
Please complete the following steps before you kickoff the elastic indexing. 

###Environment Settings
Setup the following environment variables to specify the destination elastic cluster. This assumes your elastic cluster has basic authentication enabled.
+ ES_HOST: my-elasticbeast.azure.cloud.com
+ ES_PORT: 9200
+ ES_AUTH_USER: my-username
+ ES_AUTH_PWD: mypassword

###Planet File Directory
Provide the directory location for your planet files 
+ PBF_DIRECTORY: /pbfsource
 
###Setup Elastic Indexes
