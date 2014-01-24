var PNCTypeMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var mothersToBeUpdated, entityIds;
    var deliveryOutcomeFormSubmissions;
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
                        if (doc.type === 'FormSubmission' && doc.formName === 'delivery_outcome') {
                            emit(doc, doc.entityId);
                        }
                    }
                }
            }
        );
    };

    var createAllOpenMothersView = function () {
        return dristhiRepository.createView('_design/Mother_Temp', {
                allOpenMothers: {
                    map: function (doc) {
                        if (doc.type === 'Mother' && !doc.isClosed) {
                            emit(doc, doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var getAllDeliveryOutcomes = function () {
        var deferred = q.defer();
        formRepository
            .queryView('FormSubmission_Temp/byFormNameAndEntityId', {
                include_docs: true
            })
            .then(function (response) {
                deliveryOutcomeFormSubmissions = response;
                console.log('Found ' + response.length + ' Delivery Outcomes');
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var find42DaysOldDeliveryOutcomes = function () {
        var requiredDeliveryOutcomes = _.filter(deliveryOutcomeFormSubmissions, function (r) {
            var date = _.find(r.doc.formInstance.form.fields,function (field) {
                return field.name === 'referenceDate';
            }).value
            if (Math.floor((new Date().getTime() - new Date(date).getTime()) / (1000 * 60 * 60 * 24)) <= 42) {
                console.log("Difference in days:", Math.floor((new Date().getTime() - new Date(date).getTime()) / (1000 * 60 * 60 * 24)));
                return r.doc;
            }
        });
        entityIds = _.pluck(requiredDeliveryOutcomes, 'value');
        console.log('Found ' + entityIds.length + ' Delivery Outcomes which are less than 42 days old');
    };

    var displayEntityIds = function () {
        console.log("entityIds : ", entityIds);
    };

    var getAllMothersForEntityIds = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('Mother_Temp/allOpenMothers', {
                include_docs: true
            })
            .then(function (response) {
                var openMothers = response;
                mothersToBeUpdated = _.filter(openMothers, function (openMother) {
                    return _.contains(entityIds, openMother.key.caseId);
                });
                console.log('Found ' + mothersToBeUpdated.length + ' open mothers to be updated.');
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var updatingMothersWithPNCType = function () {
        _.each(mothersToBeUpdated, function (pnc) {
            console.log('Type value :%s for %s', pnc.key.details.type, pnc.value);
            pnc.key.details.type = 'PNC';
            console.log('Added %s type value for %s.', pnc.key.details.type, pnc.value);
        });
        var deferred = q.defer();
        deferred.resolve();
        return deferred.promise;

    }
    var updateMotherDocument = function () {
        var pncs = _.map(mothersToBeUpdated, function (pnc) {
            return pnc.key;
        });
        return dristhiRepository.save(pncs);
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
            .then(createAllOpenMothersView)
            .then(getAllDeliveryOutcomes)
            .then(find42DaysOldDeliveryOutcomes)
            .then(displayEntityIds)
            .then(getAllMothersForEntityIds)
            .then(updatingMothersWithPNCType)
            .then(updateMotherDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);

    };

    return {
        migrate: migrate
    };
}

module.exports = PNCTypeMigration;