const _ = require("highland");
const geoclient = require("nyc-geoclient");
const request = require("request");
const csv = require("fast-csv");

var intersections = {};

geoclient.setApi('7e66478437762cefc8a569314c6af580','0de4b98b');

const id = function(lat, lng) {
  return `${lat},${lng}`;
};

const name = function(onStreetName, crossStreetName) {
  const onStreet = (onStreetName) ? onStreetName.toLowerCase() : '',
        crossStreet = (crossStreetName) ? crossStreetName.toLowerCase() : '';
  return `${onStreet} and ${crossStreet}`;
};

const borough = function(brgh) {
    switch (brgh.toLowerCase()) {
      case 'brooklyn':
        return geoclient.BOROUGH.BROOKLYN;
      case 'bronx':
        return geoclient.BOROUGH.BRONX;
      case 'manhattan':
        return geoclient.BOROUGH.MANHATTAN;
      case 'queens':
        return geoclient.BOROUGH.QUEENS;
      case 'staten island':
        return geoclient.BOROUGH.STATEN_ISLAND;
    }
  };

const intersectionExists = function(key) {
//  const key = getKey(onStreet, crossStreet, zipCode);
  return (intersections[key] !== undefined);
};

const setLocationDetails = function(crash, resp) {
  var intsect = JSON.parse(resp).intersection,
      latitude = intsect.latitude,
      longitude = intsect.longitude;

  if(latitude && longitude) {
    crash.lat = latitude;
    crash.lng = longitude;
    crash.hasLocation = true;
    return crash;
  }

  return crash;
};

const geocodeCrash = function(crash) {
  return new Promise((resolve, reject) => {
    geoclient.intersection(crash.onStreetName,
                    crash.crossStreetName,
                    crash.borough,
                    crash.zipCode,
                    null,
                    geoclient.RESPONSE_TYPE.json,
      (err, resp) => {
        if(err) { reject(err); }
        else if(resp) { resolve(setLocationDetails(crash, resp)); }
        else { resolve(crash); }
    });
  });
};

const lookupStreets = function(lat, lng) {
  return new Promise((resolve, reject) => {

  });
};

const formattedCrash = function(crash) {
  const onStreetName = crash["ON STREET NAME"],
        crossStreetName = crash["CROSS STREET NAME"],
        brgh = crash["BOROUGH"],
        lat = crash["LATITUDE"],
        lng = crash["LONGITUDE"],
        injuries = (crash["NUMBER OF PERSONS INJURED"]) ? parseInt(crash["NUMBER OF PERSONS INJURED"], 10) : 0,
        fatalities = (crash["NUMBER OF PERSONS KILLED"]) ? parseInt(crash["NUMBER OF PERSONS KILLED"], 10) : 0,
        zipCode = crash["ZIP CODE"],
        hasLocation = (crash["LOCATION"] !== "");
  return { injuries, onStreetName, crossStreetName,
           lat, lng, injuries, fatalities,
           zipCode, hasLocation,
           id: id(lat, lng),
           name: name(onStreetName, crossStreetName),
           borough: borough(brgh) };
};


const processCrash = function(rawCrash) {
  const crash = formattedCrash(rawCrash);
  return (crash.hasLocation) ?
    Promise.resolve(crash) : geocodeCrash(crash);
};


const getIntersections = function(url) {
  console.log(url);
  return new Promise((resolve, reject) => {
    request(url)
    .on("error", (err) => { reject(err); })
    .pipe(csv({objectMode: true, headers: true}))
    .on("data", (data) => { console.log(data); })
    .on("end", () => { console.log("done"); resolve(true); });
  });
};


getIntersections("http://hiddenfromsight.com/crashes_small.csv")
.then((rawCrashes) => {
  console.log(rawCrashes);
  running = false;
})
.catch((error) => {
  console.log('ERROR: ', error);
});
