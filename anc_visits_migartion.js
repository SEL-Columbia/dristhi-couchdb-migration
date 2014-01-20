var _ = require('underscore')._;
var q = require('q');
var cradle = require('cradle');
var FORM_DB_NAME = 'drishti-form';
var DRISTHI_DB_NAME = 'drishti';
var DB_SERVER = 'http://localhost';
var DB_PORT = 5984;
var dristhiFormDb;
var dristhiDb;

var init = function () {
    var deferred = q.defer();
    deferred.resolve();
    return deferred.promise;
};

var connectToDristhiFormDB = function () {
    var deferred = q.defer();
    dristhiFormDb = new cradle.Connection(DB_SERVER, DB_PORT, {cache: true, raw: false}).database(FORM_DB_NAME);
    deferred.resolve();
    return deferred.promise;
};

var connectToDristhiDB = function () {
    var deferred = q.defer();
    dristhiDb = new cradle.Connection(DB_SERVER, DB_PORT, {    cache: true, raw: false}).database(DRISTHI_DB_NAME);
    deferred.resolve();
    return deferred.promise;
};

var createAllFormSubmissionsByNameAndEntityIDView = function () {
    var deferred = q.defer();
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
    }, function (err, res) {
        if (err) {
            console.log('Error when creating Form Submission by name and entity id view. Message: ' + err);
            deferred.reject(err);
        }
        deferred.resolve(res);
    });
    return deferred.promise;
};

var createAllOpenANCsView = function () {
    var deferred = q.defer();
    dristhiDb.save('_design/Mother_Temp', {
        views: {
            allOpenANCs: {
                map: function (doc) {
                    if (doc.type === 'Mother' && !doc.isClosed && doc.details.type === 'ANC') {
                        emit(doc, doc.caseId);
                    }
                }
            }
        }
    }, function (err, res) {
        if (err) {
            console.log('Error when creating All Open ANCs view. Message: ' + err);
            deferred.reject(err);
        }
        deferred.resolve(res);
    });
    return deferred.promise;
};

var updateANCWithANCVisitInformation = function (response) {
    var ancVisitFormSubmissions = response.ancVisitFormSubmissions;
    var openANCs = response.openANCs;

    var ancVisits = _.map(ancVisitFormSubmissions, function (r) {
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
    var deferred = q.defer();
    deferred.resolve(openANCs);
    return deferred.promise;
};

var updateMotherDocument = function (openANCs) {
    _.each(openANCs, function (anc) {
        dristhiDb.merge(anc.id, anc.key, function (err, res) {
            if (err) {
                console.log('Could not update mother due to error. Error: ' + JSON.stringify(err) + ', Mother: ' + anc.value);
            } else {
                console.log('Updated mother: ' + anc.value);
            }
        });
    });
};

var getAllANCVisitsForANC = function (openANCs) {
    var deferred = q.defer();
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
                deferred.reject(err);
                return;
            }
            console.log('Found ' + res.length + ' ANC Visits.');
            deferred.resolve({ancVisitFormSubmissions: res, openANCs: openANCs});
        });

    return deferred.promise;
};

var getAllOpenANCs = function () {
    var deferred = q.defer();
    dristhiDb.view('Mother_Temp/allOpenANCs', function (err, response) {
        if (err) {
            console.log('Error when finding open ANCs: ' + JSON.stringify(err));
            deferred.reject(err);
        }
        deferred.resolve(response);
    });
    return deferred.promise;
};

init()
    .then(connectToDristhiFormDB)
    .then(connectToDristhiDB)
    .then(createAllFormSubmissionsByNameAndEntityIDView)
    .then(createAllOpenANCsView)
    .then(getAllOpenANCs)
    .then(getAllANCVisitsForANC)
    .then(updateANCWithANCVisitInformation)
    .then(updateMotherDocument)
;
