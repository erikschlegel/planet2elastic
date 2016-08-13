"use strict"

import pbf2json from 'pbf2json';
import elasticService from './elasticIndexer'
import denormalizer from './streams/denormalizer'
import glob from 'glob';
import featureTags from '../featureTags';

function processPbfFile(filename){
    var batchSize = 100;
    let config = {
        tags: featureTags.FEATURE_TAGS,
        leveldb: './tmp',
        batch: 5000,
        file: process.env.PBF_DIRECTORY + '/' + filename
    };

    let connection = elasticService.elasticConnection();

    pbf2json.createReadStream(config)
          .pipe(denormalizer.stream(connection));
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