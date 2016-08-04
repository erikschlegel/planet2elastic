# pbftoelastic
Pushes a set of OpenStreetMap planet files to a Tile Based Elastic Index

##Environment Settings
Setup the following environment variables to specify the destination elastic cluster. This assumes your elastic cluster has basic authentication enabled.
1. ES_HOST: my-elasticbeast.azure.cloud.com
2. ES_PORT: 9200
3. ES_AUTH_USER: my-username
4. ES_AUTH_PWD: mypassword
