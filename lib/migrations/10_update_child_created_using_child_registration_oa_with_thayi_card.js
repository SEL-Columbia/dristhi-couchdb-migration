var ChildRegistrationOAThayiCardMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var formRepository, dristhiRepository, mothers, ecs, ecsWithoutECNumber, childDocsWithoutThayiCardNumber,  childIdsWithThayiCardNumber = [],childDocsToBeUpdated = [];

    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
        formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
        dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
        deferred.resolve();
        return deferred.promise;
    };

    var createAllChildrenWithoutThayiCardView = function () {
        return dristhiRepository.createView('_design/Child_Temp', {
                allChildrenWithoutThayiCard: {
                    map: function (doc) {
                        if (doc.type === 'Child' && !doc.thayiCard) {
                            emit(doc.motherCaseId);
                        }
                    }
                }
            }
        );
    };

    var createAllMothersView = function () {
        return dristhiRepository.createView('_design/Mother_Temp', {
                allOpenMothers: {
                    map: function (doc) {
                        if (doc.type === 'Mother') {
                            emit(doc.caseId, doc.ecCaseId);
                        }
                    }
                }
            }
        );
    };

    var createAllECsWithoutECNumberView = function () {
        return dristhiRepository.createView('_design/EC_Temp', {
                allECsWithoutECNumber: {
                    map: function (doc) {
                        if (doc.type === 'EligibleCouple' && !doc.ecNumber) {
                            emit(doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var createChildRegistrationOAFormView = function () {
        return formRepository.createView('_design/FormSubmission_Temp', {
                childRegistrationOA: {
                    map: function (doc) {
                        var ecId = doc.formInstance.form.fields.filter(function (field) {
                            return field.name === 'ecId';
                        })[0].value;
                        var thayiCard = doc.formInstance.form.fields.filter(function (field) {
                            return field.name === 'thayiCardNumber';
                        })[0].value;
                        if (doc.formName === 'child_registration_oa') {
                            emit(ecId, [doc.entityId, thayiCard]);
                        }
                    }
                }
            }
        );
    };

    var getAllChildrenWithoutThayiCard = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('Child_Temp/allChildrenWithoutThayiCard', {
                include_docs: true
            })
            .then(function (response) {
//                console.log("Children without Thayi card number %s", response);
                childDocsWithoutThayiCardNumber = _.pluck(response, 'doc');
                mothers = _.pluck(response, 'key');
                mothers = _.uniq(_.reject(mothers, function (mother) {
                    return mother === null;
                }));
                console.log('Found %s Mothers.', JSON.stringify(mothers.length));
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getMothersForTheChildren = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('Mother_Temp/allOpenMothers',
            {
                keys: mothers
            })
            .then(function (response) {
                ecs = _.pluck(response, 'value');
                console.log('Found %s ECs.', ecs.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getECsWithoutECNumber = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('EC_Temp/allECsWithoutECNumber',
            {
                keys: ecs
            })
            .then(function (response) {
                ecsWithoutECNumber = _.pluck(response, 'key');
                console.log('Found %s ECs Without EC Number.', ecsWithoutECNumber.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getChildAndCorrespondingThayiCardNumber = function () {
        var deferred = q.defer();
        formRepository
            .queryView('FormSubmission_Temp/childRegistrationOA',
            {
                keys: ecsWithoutECNumber
            })
            .then(function (response) {
                childIdsWithThayiCardNumber = _.pluck(response, 'value');
                console.log("Found Child without thayi card: %s", JSON.stringify(childIdsWithThayiCardNumber));
                _.each(childIdsWithThayiCardNumber, function (childDetail) {
                    childDetail[1] = childDetail[1] === null ? _.random(1000000, 9999999).toString() : childDetail[1];
                });
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var updateThayiCardNumber = function () {
        var deferred = q.defer();

        _.each(childIdsWithThayiCardNumber, function (child) {
            var docToBeUpdated = _.find(childDocsWithoutThayiCardNumber, function (childDoc) {
                return childDoc['caseId'] === child[0];
            });
            docToBeUpdated.thayiCard = child[1];
            childDocsToBeUpdated.push(docToBeUpdated);
        });
        console.log("Child docs to be updated %s.", JSON.stringify(childDocsToBeUpdated));
        deferred.resolve();
        return deferred.promise;
    };

    var updateChildDocument = function () {
        var deferred = q.defer();
        dristhiRepository.save(childDocsToBeUpdated);
        deferred.resolve();
        return deferred.promise;
    };


    var deleteFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/FormSubmission_Temp');
    };

    var deleteEC_TempView = function () {
        return dristhiRepository.deleteDocById('_design/EC_Temp');
    };

    var deleteChild_TempView = function () {
        return dristhiRepository.deleteDocById('_design/Child_Temp');
    };

    var deleteMother_TempView = function () {
        return dristhiRepository.deleteDocById('_design/Mother_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteFormSubmission_TempView(), deleteEC_TempView(), deleteChild_TempView(), deleteMother_TempView()]);
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
            .then(createAllChildrenWithoutThayiCardView)
            .then(createAllMothersView)
            .then(createAllECsWithoutECNumberView)
            .then(createChildRegistrationOAFormView)
            .then(getAllChildrenWithoutThayiCard)
            .then(getMothersForTheChildren)
            .then(getECsWithoutECNumber)
            .then(getChildAndCorrespondingThayiCardNumber)
            .then(updateThayiCardNumber)
            .then(updateChildDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = ChildRegistrationOAThayiCardMigration;