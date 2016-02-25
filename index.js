const _ = require("highland");
const geoclient = require("nyc-geoclient");
const request = require("request");
const csv = require("fast-csv");
const fs = require("fs");

geoclient.setApi("7e66478437762cefc8a569314c6af580","0de4b98b");

var output = ``;

function id(lat, lng) {
  return `${lat},${lng}`;
};

function key(on, cross, zip) {
  return `${on}##${cross}##${zip}`;
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
           //id: id(lat, lng),
           id: key(onStreetName, crossStreetName, zipCode),
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
    .on("data", (crash) => {
      if(crash.hasLocation) {
        completeCrashes.push(crash)
      } else {
        incompleteCrashes.push(crash);
      }
      return null;
    })
    .on("end", () => {
      resolve({complete: completeCrashes, incomplete: incompleteCrashes});
    });
  });
};

function intersections(acc, intersection, idx) {
  if(acc[intersection.id]) {
    acc[intersection.id] = sumIntersection(acc[intersection.id], intersection);
  } else {
    intersection.crashes = 1;
    acc[intersection.id] = intersection;
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
  return Promise.all(incomplete.map(geocodeCrash))
    .then((crashes) => {
      return processCompletedCrashes(crashes, currentIntersections);
    });
}

function processIntersections(crashes) {
  var intersections = processCompletedCrashes(crashes.complete, {});
  var incomplete = crashes.incomplete.reduce((acc, v, i) => {
    if(intersections[v.id]) {
      intersections[v.id] = sumIntersection(intersections[v.id], v);
    } else {
      acc.push(v);
    }
    return acc;
  }, []);
  var validIncomplete = incomplete.filter(validCrash);

  output = `*******************************************
incomplete crashes: ${crashes.incomplete.length}
complete crashes: ${crashes.complete.length}
*******************************************
unmatched from incomplete crashes: ${incomplete.length}
matched from incomplete crashes: ${crashes.incomplete.length - incomplete.length}
*******************************************
invalid incomplete crashes: ${incomplete.length - validIncomplete.length}
valid incomplete crashes: ${validIncomplete.length}
*******************************************\n`
  return geocodeCrashes(validIncomplete, intersections);
}

function writeCsv(finalIntersections) {
  output += `distinct intersections: ${Object.keys(finalIntersections).length}
*******************************************\n`;
  console.log(output);

  var fileName = process.argv[3],
      writeStream = csv.createWriteStream({headers: true}),
      fileStream = fs.createWriteStream(fileName);

  fileStream.on("finish", () => {
    console.log(`Wrote ${fileName}. \n${Date()}`);
  });

  _.values(finalIntersections)
  .map((intersection) => {
    if(intersection.lat && intersection.lng) {
      intersection.id = id(intersection.lat, intersection.lng);
    }
    return intersection;
  })
  .pipe(writeStream)
  .pipe(fileStream);
}

(() => {
  var errorOut = false;

  if(!process.argv[2]) {
    console.log("must supply url to csv as first argument.");
    errorOut = true;
  }

  if(!process.argv[3]) {
    console.log("must supply filename for csv as second argument.");
    errorOut = true;
  }

  if(errorOut) {
    process.exit();
  } else {
    fetchCrashes(process.argv[2])
    .then(processIntersections)
    .then(writeCsv)
    .catch((error) => {
      console.log("ERROR: ", error);
    });
  }
}());

