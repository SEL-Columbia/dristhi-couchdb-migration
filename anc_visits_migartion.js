var _ = require('underscore')._;
var cradle = require('cradle');
var ancs = require('./ancs.js')
var ancVisits = [];
var FORM_DB_NAME = 'drishti-form';
var DRISTHI_DB_NAME = 'drishti';
var DB_SERVER = 'http://localhost';
var DB_PORT = 5984;

function ANCVisit(entityId, ancVisitNumber, ancVisitDate, weight, bpSystolic, bpDiastolic) {
    'use strict';
    var self = this;
    self.entityId = entityId;
    self.ancVisitNumber = ancVisitNumber;
    self.ancVisitDate = ancVisitDate;
    self.weight = weight;
    self.bpSystolic = bpSystolic;
    self.bpDiastolic = bpDiastolic;
}

var dristhiFormDb = new cradle.Connection(DB_SERVER, DB_PORT, {cache: true, raw: false}).database(FORM_DB_NAME);
var dristhiDb = new cradle.Connection(DB_SERVER, DB_PORT, {    cache: true, raw: false}).database(DRISTHI_DB_NAME);

var createAllFormSubmissionsByNameAndEntityIDView = function (dristhiFormDb) {
    dristhiFormDb.save('_design/FormSubmission_Temp', {
        views: {
            byFormNameAndEntityId: {
                map: function (doc) {
                    if (doc.type === 'FormSubmission' && doc.formName && doc.entityId) {
                        emit([doc.formName, doc.entityId], null);
                    }
                }
            }
        }
    });
};

var createAllOpenANCsView = function (drishtiDb) {
    drishtiDb.save('_design/Mother_Temp', {
        views: {
            allOpenANCs: {
                map: function (doc) {
                    if (doc.type === 'Mother' && !doc.isClosed && doc.details.type === 'ANC') {
                        emit(doc, doc.caseId);
                    }
                }
            }
        }
    });
};

var getAllANCVisitsForANC = function (dristhiFormDb, dristhiDb, openANCs) {
    var entityIds = _.pluck(openANCs, 'value');
    console.log('Found ' + entityIds.length + ' open ANCs.');

    dristhiFormDb.view('FormSubmission_Temp/byFormNameAndEntityId',
        {
            keys: _.map(entityIds, function (entityId) {
                return ['anc_visit', entityId];
            }),
            include_docs: true
        },
        function (err, res) {
            if (err) {
                console.log('Error when getting ANC visit forms: ' + JSON.stringify(err));
                return;
            }
            console.log('Found ' + res.length + ' ANC Visits.');

            var ancVisits = _.map(res, function (r) {
                var fields = _.filter(r.doc.formInstance.form.fields, function (field) {
                    return field.name === 'ancVisitDate' ||
                        field.name === 'weight' ||
                        field.name === 'bpSystolic' ||
                        field.name === 'bpDiastolic';
                });
                return {
                    entityId: r.doc.entityId,
                    ancVisitDate: _.find(fields,function (field) {
                        return field.name === 'ancVisitDate';
                    }).value,
                    weight: _.find(fields,function (field) {
                        return field.name === 'weight';
                    }).value,
                    bpSystolic: _.find(fields,function (field) {
                        return field.name === 'bpSystolic';
                    }).value,
                    bpDiastolic: _.find(fields,function (field) {
                        return field.name === 'bpDiastolic';
                    }).value
                }
            });
//            console.log(JSON.stringify(ancVisits));
            _.each(openANCs, function (anc) {
                var allANCVisitsForMother = _.where(ancVisits, {entityId: anc.value});
                _.each(allANCVisitsForMother, function (ancVisit) {
                    delete ancVisit.entityId;
                });
                anc.key.ancVisits = allANCVisitsForMother;
            });
            console.log("Updated ANCs: " + JSON.stringify(openANCs));
            _.each(openANCs, function (anc) {
                dristhiDb.merge(anc.id, anc.key, function (err, res) {
                    if (err) {
                        console.log('Could not update mother due to error. Error: ' + JSON.stringify(err) + ', Mother: ' + anc.value);
                    } else {
                        console.log('Updated mother: ' + anc.value);
                    }
                });
            });
        });
};

var getAllOpenANCs = function (dristhiDb) {
    return dristhiDb.view('Mother_Temp/allOpenANCs', function (err, response) {
        if (err) {
            console.log('Error when finding open ANCs: ' + JSON.stringify(err));
            return;
        }
//        console.log("Mothers: " + JSON.stringify(response));
//        console.log("EntityIds: " + entityIds);
        getAllANCVisitsForANC(dristhiFormDb, dristhiDb, response);
    });
};


createAllFormSubmissionsByNameAndEntityIDView(dristhiFormDb);
createAllOpenANCsView(dristhiDb);
getAllOpenANCs(dristhiDb);


//requiredANCs.forEach(function(anc) {
//    anc.key.ancVisits = _.filter(ancVisits, function (ancVisit) {
//        return anc.key.caseId === ancVisit.entityId;
//    });
//    anc.key.ancVisits.forEach(function (ancVisit){ delete ancVisit.entityId; });
//});







