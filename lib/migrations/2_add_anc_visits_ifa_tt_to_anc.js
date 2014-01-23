var ANCMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var openANCs;
    var ancVisitFormSubmissions;
    var ifaFormSubmissions;
    var ttFormSubmissions;
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
                            emit(doc, doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var getAllOpenANCs = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('Mother_Temp/allOpenANCs')
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

    var getAllANCVisitsForANC = function () {
        var deferred = q.defer();
        var entityIds = _.pluck(openANCs, 'value');
        formRepository
            .queryView('FormSubmission_Temp/byFormNameAndEntityId',
            {
                keys: _.map(entityIds, function (entityId) {
                    return ['anc_visit', entityId];
                }),
                include_docs: true
            })
            .then(function (response) {
                ancVisitFormSubmissions = response;
                console.log('Found %s ANC Visits.', response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getAllIFAsForANC = function () {
        var deferred = q.defer();
        var entityIds = _.pluck(openANCs, 'value');
        formRepository
            .queryView('FormSubmission_Temp/byFormNameAndEntityId',
            {
                keys: _.map(entityIds, function (entityId) {
                    return ['ifa', entityId];
                }),
                include_docs: true
            })
            .then(function (response) {
                ifaFormSubmissions = response;
                console.log('Found %s IFA.', response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getAllTTsForANC = function () {
        var deferred = q.defer();
        var entityIds = _.pluck(openANCs, 'value');
        var keysForTT = [];
        _.each(entityIds, function (entityId) {
            keysForTT.push(['tt_1', entityId]);
            keysForTT.push(['tt_2', entityId]);
            keysForTT.push(['tt_booster', entityId]);
        });
        formRepository
            .queryView('FormSubmission_Temp/byFormNameAndEntityId',
            {
                keys: keysForTT,
                include_docs: true
            })
            .then(function (response) {
                ttFormSubmissions = response;
                console.log('Found %s TTs.', response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var updateANCWithANCVisitInformation = function () {
        var deferred = q.defer();
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
        deferred.resolve();
        return deferred.promise;
    };

    var updateANCWithIFAInformation = function () {
        var deferred = q.defer();
        var ifas = _.map(ifaFormSubmissions, function (r) {
            var fields = _.filter(r.doc.formInstance.form.fields, function (field) {
                return field.name === 'ifaTabletsDate' ||
                    field.name === 'numberOfIFATabletsGiven';
            });
            return {
                entityId: r.doc.entityId,
                numberOfIFATabletsGiven: _.find(fields,function (field) {
                    return field.name === 'numberOfIFATabletsGiven';
                }).value,
                ifaTabletsDate: _.find(fields,function (field) {
                    return field.name === 'ifaTabletsDate';
                }).value
            };
        });
        _.each(openANCs, function (anc) {
            var allIFAsForMother = _.where(ifas, {entityId: anc.value});
            _.each(allIFAsForMother, function (ifa) {
                delete ifa.entityId;
            });
            console.log('Added %s IFA to ANC %s.', allIFAsForMother.length, anc.value);
            anc.key.ifas = allIFAsForMother;
        });
        deferred.resolve();
        return deferred.promise;
    };

    var updateANCWithTTInformation = function () {
        var deferred = q.defer();
        var tts = _.map(ttFormSubmissions, function (r) {
            var fields = _.filter(r.doc.formInstance.form.fields, function (field) {
                return field.name === 'ttDose' ||
                    field.name === 'ttDate';
            });
            return {
                entityId: r.doc.entityId,
                ttDose: _.find(fields,function (field) {
                    return field.name === 'ttDose';
                }).value,
                ttDate: _.find(fields,function (field) {
                    return field.name === 'ttDate';
                }).value
            };
        });
        _.each(openANCs, function (anc) {
            var allTTsForMother = _.where(tts, {entityId: anc.value});
            _.each(allTTsForMother, function (tt) {
                delete tt.entityId;
            });
            console.log('Added %s TT to ANC %s.', allTTsForMother.length, anc.value);
            anc.key.tts = allTTsForMother;
        });
        deferred.resolve();
        return deferred.promise;
    };

    var updateMotherDocument = function () {
        var deferred = q.defer();
        var mothers = _.map(openANCs, function (mother) {
            return mother.key;
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
            .then(getAllANCVisitsForANC)
            .then(updateANCWithANCVisitInformation)
            .then(getAllIFAsForANC)
            .then(updateANCWithIFAInformation)
            .then(getAllTTsForANC)
            .then(updateANCWithTTInformation)
            .then(updateMotherDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = ANCMigration;