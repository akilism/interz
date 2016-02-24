const _ = require("highland");
const geoclient = require("nyc-geoclient");
const request = require("request");
const csv = require("fast-csv");

geoclient.setApi("7e66478437762cefc8a569314c6af580","0de4b98b");

function id(lat, lng) {
  return `${lat},${lng}`;
};

function name(onStreetName, crossStreetName) {
  const onStreet = (onStreetName) ? onStreetName.toLowerCase() : "",
        crossStreet = (crossStreetName) ? crossStreetName.toLowerCase() : "";
  return `${onStreet} and ${crossStreet}`;
};

function borough(brgh) {
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

function intersectionExists(key) {
  return (intersections[key] !== undefined);
};

function setLocationDetails(crash, resp) {
  var intsect = JSON.parse(resp).intersection,
      latitude = intsect.latitude,
      longitude = intsect.longitude;

  if(latitude && longitude) {
    crash.lat = latitude;
    crash.lng = longitude;
    crash.id = id(latitude, longitude);
    crash.hasLocation = true;
    return crash;
  }

  return crash;
};

function sumIntersection(a, b) {
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
    crashes: a["crashes"]++,
    borough: (a["borough"]) ? a["borough"] : b["borough"]};
};

function geocodeCrash(crash) {
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

function lookupStreets(lat, lng) {
  return new Promise((resolve, reject) => {

  });
};

function formattedCrash(crash) {
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

function fetchCrashes(url) {
  return new Promise((resolve, reject) => {
  	var completeCrashes = [],
  			incompleteCrashes = [];
  	console.log(url);

    request(url)
    .on("error", (err) => { reject(err); })
    .pipe(csv({objectMode: true, headers: true}))
    .pipe(_().map(formattedCrash))
    .on('data', (crash) => {
      if(crash.hasLocation) {
        completeCrashes.push(crash)
      } else {
        incompleteCrashes.push(crash);
      }
      return null;
    })
    .on("end", () => {
     console.log("complete crashes:", completeCrashes.length);
     console.log("incomplete crashes:", incompleteCrashes.length);
      resolve({complete: completeCrashes, incomplete: incompleteCrashes});
    });
  });
};

function intersections(acc, v, i) {
  if(acc[v.id]) {
    acc[v.id] = sumIntersection(acc[v.id], v);
  } else {
    v.crashes = 1;
    acc[v.id] = v;
  }
  return acc;
}

function processCompletedCrashes(crashes, currentIntersections) {
  return crashes.reduce(intersections, currentIntersections);
}

function validCrash(crash) {
  return crash.borough && crash.onStreetName && crash.crossStreetName && crash.zipCode;
}

function geocodeCrashes(incomplete, currentIntersections) {
  //batch these calls to geocodeCrash
  return Promise.all(incomplete.filter(validCrash).map(geocodeCrash))
    .then((crashes) => {
      return processCompletedCrashes(crashes, currentIntersections);
    });
}

fetchCrashes("http://hiddenfromsight.com/crashes_medium.csv")
.then((crashes) => {
  var intersections = processCompletedCrashes(crashes.complete, {});
  console.log(crashes.complete.length, ':', Object.keys(intersections).length);
  return geocodeCrashes(crashes.incomplete, intersections);
})
.then((finalIntersections) => {
  console.log('final: ', Object.keys(finalIntersections).length);
})
.catch((error) => {
  console.log("ERROR: ", error);
});
