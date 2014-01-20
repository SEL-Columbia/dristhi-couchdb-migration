var openANCs = [];
var requiredANCs = [];
var _ = require('underscore')._;

function filterByEcIds(ecIds) {
    console.log(ecIds);
    return _.filter(openANCs, function (openANC) {
        return  _.contains(ecIds, openANC.key.caseId);
    });
}
function createDataBaseConnection(cradle) {
    var drishtiDb = new (cradle.Connection)(('http://localhost', 5984, {
        cache: true,
        raw: false
    })).database('drishti');
    return drishtiDb;
}

function isDataBaseExists(drishtiDb) {
    drishtiDb.exists(function (err, exists) {
        if (err) {
            console.log('error', err);
        } else if (exists) {
            console.log('the force is with you.');
        } else {
            console.log('database does not exists.');
            drishtiDb.create();
        }
    });
}

function createAllOpenANCsView(drishtiDb) {
    drishtiDb.save('_design/Mothers', {
        views: {
            byOpenANCs: {
                map: function (doc) {
                    if (doc.type === 'Mother' && !doc.isClosed && doc.details.type === 'ANC') {
                        emit(doc, null);
                    }
                }
            }
        }
    });
}

function modifyMotherDetails(drishtiDb, ecIds, ancVisits) {
    drishtiDb.view('Mothers/byOpenANCs', function (err, res) {
        console.log("ANC visits res: " + res);
        res.forEach(function (row) {
            openANCs.push(row);
        });
        requiredANCs = filterByEcIds(ecIds);
        requiredANCs.forEach(function (anc) {
            anc.key.ancVisits = _.filter(ancVisits, function (ancVisit) {
                return anc.key.caseId === ancVisit.entityId;
            });
            anc.key.ancVisits.forEach(function (ancVisit) {
                delete ancVisit.entityId;
            });
        });
        drishtiDb.save(requiredANCs, function (err, res) {
            console.log("Response: " + res);
        });
    });
}

var getANC = function (ecIds, ancVisits) {
    var cradle = require('cradle');
    var drishtiDb = createDataBaseConnection(cradle);
    isDataBaseExists(drishtiDb);
    createAllOpenANCsView(drishtiDb);
    modifyMotherDetails(drishtiDb, ecIds, ancVisits);
};

exports.getANC = getANC;
//exports.filterByEcIds = filterByEcIds;





