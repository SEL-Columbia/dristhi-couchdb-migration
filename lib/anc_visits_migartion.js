var _ = require('underscore')._;
var q = require('q');
var FORM_DB_NAME = 'drishti-form';
var DRISTHI_DB_NAME = 'drishti';
var DB_SERVER = 'http://localhost';
var DB_PORT = 5984;
var openANCs;
var ancVisitFormSubmissions;
var formRepository;
var dristhiRepository;

var connectToDB = function () {
    var deferred = q.defer();
    var Repository = require('./repository');
    formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
    dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
    deferred.resolve();
    return deferred.promise;
};

var createAllFormSubmissionsByNameAndEntityIDView = function () {
    return formRepository.createView('_design/FormSubmission_Temp', {
            byFormNameAndEntityId: {
                map: function (doc) {
                    if (doc.type === 'FormSubmission' && doc.formName && doc.entityId) {
                        emit([doc.formName, doc.entityId], null);
                    }
                }
            }
        }
    );
};

var createAllOpenANCsView = function () {
    return dristhiRepository.createView('_design/Mother_Temp', {
            allOpenANCs: {
                map: function (doc) {
                    if (doc.type === 'Mother' && !doc.isClosed && doc.details.type === 'ANC') {
                        emit(doc, doc.caseId);
                    }
                }
            }
        }
    );
};

var getAllOpenANCs = function () {
    return dristhiRepository.queryView('Mother_Temp/allOpenANCs');
};

var getAllANCVisitsForANC = function (response) {
    openANCs = response;
    var entityIds = _.pluck(openANCs, 'value');
    console.log('Found ' + entityIds.length + ' open ANCs.');
    return formRepository.queryView('FormSubmission_Temp/byFormNameAndEntityId',
        {
            keys: _.map(entityIds, function (entityId) {
                return ['anc_visit', entityId];
            }),
            include_docs: true
        });
};

var updateANCWithANCVisitInformation = function (response) {
    console.log('Found %s ANC Visits.', response.length);
    ancVisitFormSubmissions = response;

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
    var ancs = _.map(openANCs, function (anc) {
        return anc.key;
    });
    return dristhiRepository.save(ancs);
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
