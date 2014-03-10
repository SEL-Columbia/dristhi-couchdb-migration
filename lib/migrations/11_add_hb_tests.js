var ANCMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var openANCs;
    var hbTestFormSubmissions;
    var formRepository;
    var dristhiRepository;

    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
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
                        if (doc.type === 'Mother' && doc.isClosed === 'false' && doc.details.type === 'ANC') {
                            emit(doc._id, doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var getAllOpenANCs = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('Mother_Temp/allOpenANCs',{
                include_docs: true
            })
            .then(function (response) {
                openANCs = response;
                var entityIds = _.pluck(openANCs, 'value');
                console.log('Found ' + entityIds.length + ' open ANCs.');
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getAllHBTestsForANC = function () {
        var deferred = q.defer();
        var entityIds = _.pluck(openANCs, 'value');
        formRepository
            .queryView('FormSubmission_Temp/byFormNameAndEntityId',
            {
                keys: _.map(entityIds, function (entityId) {
                    return ['hb_test', entityId];
                }),
                include_docs: true
            })
            .then(function (response) {
                hbTestFormSubmissions = response;
                console.log('Found %s HB Tests.', response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var updateANCWithHBTestInformation = function () {
        var deferred = q.defer();
        var hbTests = _.map(hbTestFormSubmissions, function (r) {
            var fields = _.filter(r.doc.formInstance.form.fields, function (field) {
                return field.name === 'hbTestDate' ||
                    field.name === 'hbLevel';
            });
            return {
                entityId: r.doc.entityId,
                hbTestDate: _.find(fields,function (field) {
                    return field.name === 'hbTestDate';
                }).value,
                hbLevel: _.find(fields,function (field) {
                    return field.name === 'hbLevel';
                }).value
            };
        });
        _.each(openANCs, function (anc) {
            var allHBTestsForMother = _.where(hbTests, {entityId: anc.value});
            _.each(allHBTestsForMother, function (hbTest) {
                delete hbTest.entityId;
            });
            anc.doc.hbTests = allHBTestsForMother;
            console.log('Added %s HB Tests to ANC %s.', allHBTestsForMother.length, anc.value);
        });
        deferred.resolve();
        return deferred.promise;
    };

    var updateMotherDocument = function () {
        var deferred = q.defer();
        var mothers = _.map(openANCs, function (mother) {
            return mother.doc;
        });
        dristhiRepository.save(mothers);
        deferred.resolve();
        return deferred.promise;
    };

    var deleteMother_TempView = function () {
        return dristhiRepository.deleteDocById('_design/Mother_Temp');
    };

    var deleteFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/FormSubmission_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteMother_TempView(), deleteFormSubmission_TempView()]);
    };

    var reportMigrationComplete = function () {
        var deferred = q.defer();
        deferred.resolve();
        console.log("Migration complete.");
        return deferred.promise;
    };

    var reportMigrationFailure = function (err) {
        var deferred = q.defer();
        deferred.resolve();
        console.error("Migration Failed. Error: %s.", JSON.stringify(err));
        return deferred.promise;
    };

    var migrate = function () {
        connectToDB()
            .then(createAllFormSubmissionsByNameAndEntityIDView)
            .then(createAllOpenANCsView)
            .then(getAllOpenANCs)
            .then(getAllHBTestsForANC)
            .then(updateANCWithHBTestInformation)
            .then(updateMotherDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = ANCMigration;