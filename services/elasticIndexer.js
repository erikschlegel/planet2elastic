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
            log: {
                type: 'file',
                level: 'info',
                path: './logs/elasticsearch.log'
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

function elasticBulkIndex(connection, tilesToIndex, placesToIndex, callback){
    let placeIndexHeader = {
        index:  { _index: 'places', _type: 'place'}
    };

    let tilesIndexHeader = {
        index:  { _index: 'tiles', _type: 'geotile'}
    };

    let body = [];

    body = body.concat(transformToElasticBulk(tilesToIndex, tilesIndexHeader));
    body = body.concat(transformToElasticBulk(placesToIndex, placeIndexHeader));
    console.log('Requesting ' + body.length + ' for elastic indexing.');

    connection.bulk({
        body: body
    },
    (err, response) => {
        if(err){
          console.error('An error occured while trying to index ' + body.length + 'documents to elastic');
          console.error(JSON.stringify(err));
        }else{
            console.log('Succesfully indexed ' + body.length + ' documents. Took: ' + response.took);
        }

        callback();
    });
}

module.exports = {
    elasticBulkIndex: elasticBulkIndex,
    elasticConnection: elasticClient
}