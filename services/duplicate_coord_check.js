function duplicateCoordsExists(coordinates){
    
    for(let i = 0; i < coordinates.length - 1; i++){
        let coord = coordinates[i];

        for(let s = i + 1; s < coordinates.length; s++){
            let testCoord = coordinates[s];
 
            if(coord[0] == testCoord[0] && coord[1] == testCoord[1]){
                return true;
            }
        }
    }

    return false;
}

module.exports = {
    duplicateExists: duplicateCoordsExists
}