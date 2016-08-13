import process from 'process';
import elasticsearch from 'elasticsearch';

function VerifyEnvironmentVarsExist(envVars){
    if(!envVars){
        throw new Error("envVars is undefined.");
    }

    envVars.map(item => {
        if(!process.env[item])
          throw new Error(item + " environment variabe is not defined");
    });

    return true;
}

function elasticClient(){
    if(VerifyEnvironmentVarsExist(["ES_HOST", "ES_PORT", "ES_AUTH_USER", "PBF_DIRECTORY", "ES_AUTH_PWD"])){
        return new elasticsearch.Client({
            host: [
                {
                host: process.env.ES_HOST,
                auth: process.env.ES_AUTH_USER + ":" + process.env.ES_AUTH_PWD
                }
            ],
            keepAlive: true,
            log: {
                  type: 'stdio',
                  levels: ['error', 'warning']
            },
            requestTimeout: 20000
        });
    }
}

function transformToElasticBulk(collection, header){
    let body = [];

    for(let [key, value] of collection){
        let hdr_tmp = JSON.parse(JSON.stringify(header));
        if(!value.id){
            console.error('Missed document ID. Skipping ' + key);
            console.log(JSON.stringify(value));
            console.log(JSON.stringify(header));
            process.exit();
        }

        hdr_tmp.index['_id'] = value.id;

        body.push(hdr_tmp);
        body.push(value);
    }

    return body;
}

function defaultErrorHandler(response, placesToIndex){
          response.items.map(item => {
              if(item.index && item.index.status > 201){
                  console.error('An elastic error occured while trying to index document ' + item.index["_id"]  + ' to ' + item.index["_index"]);
                  console.log(JSON.stringify(item));
                  let place = placesToIndex.get(item.index["_id"]);
                  console.log(JSON.stringify(place));
              }
          });
}

function invokeElasticSearchQuery(connection, index, type, queryBody, callback){
    connection.search({
                    index: index,
                    type: type,
                    body: queryBody
                }, (error, response) => {
                    callback(response);
                }
    );
}

function elasticBulkIndex(connection, tilesToIndex, placesToIndex, callback, errorHandler){
    let placeIndexHeader = {
        index:  { _index: 'places', _type: 'place'}
    };

    let tilesIndexHeader = {
        index:  { _index: 'tiles', _type: 'geotile'}
    };

    let body = [];

    body = body.concat(transformToElasticBulk(tilesToIndex, tilesIndexHeader));
    body = body.concat(transformToElasticBulk(placesToIndex, placeIndexHeader));

    connection.bulk({
        body: body
    },
    (err, response) => {
        if(response.errors){
            if (errorHandler && typeof(errorHandler) == "function"){
                errorHandler(response, placesToIndex);
            }else{
                defaultErrorHandler(response, placesToIndex);
            }
        }else{
            console.log('Succesfully indexed ' + body.length + ' documents. Took: ' + response.took);
        }

        callback();
    });
}

module.exports = {
    elasticBulkIndex: elasticBulkIndex,
    elasticConnection: elasticClient,
    invokeElasticSearchQuery: invokeElasticSearchQuery
}