const _ = require("highland");
const geoclient = require("nyc-geoclient");
const request = require("request");
const csv = require("fast-csv");

var intersections = {};

geoclient.setApi("7e66478437762cefc8a569314c6af580","0de4b98b");

const id = function(lat, lng) {
  return `${lat},${lng}`;
};

const name = function(onStreetName, crossStreetName) {
  const onStreet = (onStreetName) ? onStreetName.toLowerCase() : "",
        crossStreet = (crossStreetName) ? crossStreetName.toLowerCase() : "";
  return `${onStreet} and ${crossStreet}`;
};

const borough = function(brgh) {
    switch (brgh.toLowerCase()) {
      case "brooklyn":
        return geoclient.BOROUGH.BROOKLYN;
      case "bronx":
        return geoclient.BOROUGH.BRONX;
      case "manhattan":
        return geoclient.BOROUGH.MANHATTAN;
      case "queens":
        return geoclient.BOROUGH.QUEENS;
      case "staten island":
        return geoclient.BOROUGH.STATEN_ISLAND;
    }
  };

const intersectionExists = function(key) {
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

const sumIntersection = function(a, b) {
	return { onStreetName: (a["onStreetName"]) ? a["onStreetName"] : b["onStreetName"],
		crossStreetName: (a["crossStreetName"]) ? a["crossStreetName"] : b["crossStreetName"],
    lat: (a["lat"]) ? a["lat"] : b["lat"],
    lng: (a["lng"]) ? a["lng"] : b["lng"],
    injuries: a["injuries"] + b["injuries"],
    fatalities: a["fatalities"] + b["fatalities"],
    zipCode: (a["zipCode"]) ? a["zipCode"] : b["zipCode"],
    hasLocation: a["hasLocation"],
    id: a["id"],
    name: a["name"],
    borough: (a["borough"]) ? a["borough"] : b["borough"]};
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
	console.log(crash);
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
  return (crash.hasLocation) ?
    Promise.resolve(crash) : geocodeCrash(crash);
};


// .pipe(_().map((s) => {
    // 	console.log(s.toString());
    // 	return s;
    // }))
    // .map((crash) => {
    // 	console.log(crash.hasLocation);
    // 	if(crash.hasLocation) {
    // 		completeCrashes.push(crash);
    // 	} else {
    // 		incompleteCrashes.push(crash);
    // 	}
    // 	return crash;
    // })
    // .pipe(_())
    // .on("data", (data) => { data.then((crash) => {
    // 	return crash;
    // }); })

const processCrashes = function(url) {
  return new Promise((resolve, reject) => {
  	var completeCrashes = [],
  			incompleteCrashes = [];
  	console.log(url);
    request(url)
    // .on("error", (err) => { reject(err); })
    // .pipe(csv({objectMode: true, headers: true}))
    .pipe(_().map((crash) => {
    	console.log("crash:", crash);
    	return crash;
    	// return formattedCrash(crash);
    }))
    .on("end", () => {
    	console.log("complete crashes:", completeCrashes.length);
    	console.log("incomplete crashes:", incompleteCrashes.length);
    	resolve(true);
    });
  });
};


processCrashes("http://hiddenfromsight.com/crashes_small.csv")
.then((rawCrashes) => {
  console.log(rawCrashes);
})
.catch((error) => {
  console.log("ERROR: ", error);
});
