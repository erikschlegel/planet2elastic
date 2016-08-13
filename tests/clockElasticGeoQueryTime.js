var elasticService = require('../services/elasticIndexer');

function runGeoDistanceQuery(connection, coordinatePt, distanceMeters){
    let callback = rsp => {
        console.log('Query returned ' + rsp.hits.total + ' docouments in ' + rsp.took + ' ms');
    };

    if(!coordinatePt.lat || !coordinatePt.lon){
        throw new Error("Expecting coordinatePt to have a lat and lon property");
    }

    let distance = distanceMeters + 'm';

    let distanceQuery = [
                            {
                                "geo_distance" : {
                                    "distance": distance,
                                    "distance_type": "plane",
                                    "location" : coordinatePt
                                 }
                            }
                        ];

     let queryBody = {   
        "query": {
            "bool": {
                "should" : distanceQuery,
            }
        }
    }

    //console.log('Invoking query: ' + JSON.stringify(queryBody));
    elasticService.invokeElasticSearchQuery(connection, "places", "place", queryBody, callback);

}

function mainTest(){
    let connection = elasticService.elasticConnection();
    var testCoordinates = [
        {lat: 47.9872259, lon: 7.6563317},
        {lat: 40.7382913, lon:-73.9988291},
        {lat: 41.818771, lon:-71.417044}
    ];

    let runs = 100;

    let meterDistance = 500;

    for(var i = 0; i <= runs; i++ ){
        testCoordinates.map(point => {
            runGeoDistanceQuery(connection, point, meterDistance);
        });
    }
}

module.exports = {
    mainTest: mainTest
}