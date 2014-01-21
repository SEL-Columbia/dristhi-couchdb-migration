var _ = require('underscore')._;
var q = require('q');
var cradle = require('cradle');
var FORM_DB_NAME = 'drishti-form';
var DRISTHI_DB_NAME = 'drishti';
var DB_SERVER = 'http://localhost';
var DB_PORT = 5984;
var repository = new (require('./repository'))();
var dristhiFormDb;
var dristhiDb;

var connectToDB = function () {
    var deferred = q.defer();
    dristhiFormDb = repository.connectToDB(DB_SERVER, DB_PORT, FORM_DB_NAME);
    dristhiDb = repository.connectToDB(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
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
            console.error('Error when creating Form Submission by name and entity id view. Message: %s.', err);
            deferred.reject(err);
        }
        console.log('Created view: FormSubmission_Temp.byFormNameAndEntityId.');
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
            console.error('Error when creating All Open ANCs view. Message: %s.', err);
            deferred.reject(err);
        }
        console.log('Created view: Mother_Temp.allOpenANCs.');
        deferred.resolve(res);
    });
    return deferred.promise;
};

var getAllOpenANCs = function () {
    var deferred = q.defer();
    dristhiDb.view('Mother_Temp/allOpenANCs', function (err, response) {
        if (err) {
            console.error('Error when finding open ANCs: %s.', JSON.stringify(err));
            deferred.reject(err);
        }
        deferred.resolve(response);
    });
    return deferred.promise;
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
                console.error('Error when getting ANC visit forms: %s.', JSON.stringify(err));
                deferred.reject(err);
                return;
            }
            console.log('Found ' + res.length + ' ANC Visits.');
            deferred.resolve({ancVisitFormSubmissions: res, openANCs: openANCs});
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
        };
    });
    _.each(openANCs, function (anc) {
        var allANCVisitsForMother = _.where(ancVisits, {entityId: anc.value});
        _.each(allANCVisitsForMother, function (ancVisit) {
            delete ancVisit.entityId;
        });
        console.log('Added %s ANC Visits to ANC %s.', allANCVisitsForMother.length, anc.value);
        anc.key.ancVisits = allANCVisitsForMother;
    });
    var deferred = q.defer();
    deferred.resolve(openANCs);
    return deferred.promise;
};

var updateMotherDocument = function (openANCs) {
    var deferred = q.defer();
    var ancs = _.map(openANCs, function (anc) {
        return anc.key;
    });
    dristhiDb.save(ancs, function (err, res) {
        if (err) {
            deferred.reject('Error when bulk updating ANCs: ' + JSON.stringify(err));
        }
        var notUpdatedANCs = _.filter(res, function (item) {
            return _.has(item, 'error');
        });
        if (notUpdatedANCs.length > 0) {
            deferred.reject('Unable to update following ANCs: ' + JSON.stringify(notUpdatedANCs));
        }
        console.log('Updated %s ANCs with ANC visit information', openANCs.length);
        deferred.resolve();
    });
    return deferred.promise;
};

var reportMigrationComplete = function () {
    console.log("Migration complete.");
};

var reportMigrationFailure = function (err) {
    console.error("Migration Failed. Error: %s.", err);
};

connectToDB()
    .then(createAllFormSubmissionsByNameAndEntityIDView)
    .then(createAllOpenANCsView)
    .then(getAllOpenANCs)
    .then(getAllANCVisitsForANC)
    .then(updateANCWithANCVisitInformation)
    .then(updateMotherDocument)
    .then(reportMigrationComplete, reportMigrationFailure);
