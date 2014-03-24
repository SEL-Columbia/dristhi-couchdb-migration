var ChildImmunizationsMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var openChildren;
    var childImmunizationsFormSubmissions;
    var vitaminAFormSubmissions;
    var childRegistrationECFormSubmissions;
    var childRegistrationOAFormSubmissions;
    var pncRegistrationOAFormSubmissions;
    var deliveryOutcomeFormSubmissions;
    var formRepository;
    var dristhiRepository;
    var allImmunizationsWithEntityIdAndDate = [];
    var allVitaminsWithEntityIdAndDate = [];
    var immunizationsAndDateMap = {bcg: 'bcgDate', opv_0: 'opv0Date', hepb_0: 'hepb0Date', opv_1: 'opv1Date', pentavalent_1: 'pentavalent1Date', opv_2: 'opv2Date', pentavalent_2: 'pentavalent2Date', opv_3: 'opv3Date', pentavalent_3: 'pentavalent3Date', measles: 'measlesDate', je: 'jeDate', mmr: 'mmrDate', dptbooster_1: 'dptBooster1Date', opvbooster: 'opvBoosterDate', measlesbooster: 'measlesBoosterDate', je_2: 'je2Date', dptbooster_2: 'dptBooster2Date', vitamin_a1: "vitamin1Date", vitamin_a2: "vitamin2Date", vitamin_a3: "vitamin3Date", vitamin_a4: "vitamin4Date", vitamin_a5: "vitamin5Date", vitamin_a6: "vitamin6Date", vitamin_a7: "vitamin7Date", vitamin_a8: "vitamin8Date", vitamin_a9: "vitamin9Date"};

    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
        formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
        dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
        deferred.resolve();
        return deferred.promise;
    };

    var createChildRegistrationECFormSubmissionsView = function () {
        return formRepository.createView('_design/ChildRegistrationEC_FormSubmission_Temp', {
                byFormName: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission' && doc.formName === 'child_registration_ec') {
                            emit(doc.formInstance.form.fields[2].value);
                        }
                    }
                }
            }
        );
    };

    var createChildRegistrationOAFormSubmissionsView = function () {
        return formRepository.createView('_design/ChildRegistrationOA_FormSubmission_Temp', {
                byFormName: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission' && doc.formName === 'child_registration_oa') {
                            emit(doc.formInstance.form.fields[0].value);
                        }
                    }
                }
            }
        );
    };

    var createDeliveryOutcomeFormSubmissionsView = function () {
        return formRepository.createView('_design/DeliveryOutcome_FormSubmission_Temp', {
                byFormName: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission' && doc.formName === 'delivery_outcome') {
                            emit(null);
                        }
                    }
                }
            }
        );
    };

    var createPNCRegistrationOAFormSubmissionsView = function () {
        return formRepository.createView('_design/PNCRegistrationOA_FormSubmission_Temp', {
                byFormName: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission' && doc.formName === 'pnc_registration_oa') {
                            emit(null);
                        }
                    }
                }
            }
        );
    };

    var createChildImmunizationsFormSubmissionsByNameView = function () {
        return formRepository.createView('_design/ChildImmunizations_FormSubmission_Temp', {
                byFormName: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission' && doc.formName === 'child_immunizations') {
                            emit(doc.entityId);
                        }
                    }
                }
            }
        );
    };

    var createVitaminAFormSubmissionsByNameView = function () {
        return formRepository.createView('_design/VitaminA_FormSubmission_Temp', {
                byFormName: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission' && doc.formName === 'vitamin_a') {
                            emit(doc.entityId);
                        }
                    }
                }
            }
        );
    };

    var createAllOpenChildrenView = function () {
        return dristhiRepository.createView('_design/Child_Temp', {
                allOpenChildren: {
                    map: function (doc) {
                        if (doc.type === 'Child' && doc.isClosed === 'false') {
                            emit(doc._id, doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var getAllOpenChildren = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('Child_Temp/allOpenChildren', {
                include_docs: true
            })
            .then(function (response) {
                openChildren = response;
                console.log('Found ' + response.length + ' open Children.');
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getChildImmunizationsInChildRegistrationEC = function () {
        var deferred = q.defer();
        var childIds = _.pluck(openChildren, 'value');

        formRepository.queryView('ChildRegistrationEC_FormSubmission_Temp/byFormName', {
            keys: childIds,
            include_docs: true
        }).then(function (response) {
            childRegistrationECFormSubmissions = response;
            console.log('Found %s Child EC Form Submissions.', response.length);
            deferred.resolve();
        }, function (error) {
            deferred.reject(error);
        });
        return deferred.promise;
    };

    var getMapOfImmunizationsAndEntityIdsFromChildRegistrationEC = function () {
        var deferred = q.defer();

        var childImmunizationAndVitaminDetails = getImmunizationsFromForm(childRegistrationECFormSubmissions);
        allImmunizationsWithEntityIdAndDate.push(_.pluck(childImmunizationAndVitaminDetails, 'immunizationsWithDate'));
        allVitaminsWithEntityIdAndDate.push(_.pluck(childImmunizationAndVitaminDetails, 'vitaminDosesWithDate'));
        deferred.resolve();
        return deferred.promise;
    };

    var getImmunizationsFromForm = function (formSubmissions) {
        return _.map(formSubmissions, function (r) {
            var immunizationsDetails = _.find(r.doc.formInstance.form.fields, function (field) {
                return field.name === 'immunizationsGiven';
            });

            var vitaminHistory = _.find(r.doc.formInstance.form.fields, function (field) {
                return field.name === 'childVitaminAHistory';
            });
            var vitaminDoses = [];
            if (vitaminHistory.value != undefined) {
                vitaminDoses = vitaminHistory.value.split(" ");
            }

            var immunizationsGiven = [];
            if (immunizationsDetails.value != undefined) {
                immunizationsGiven = immunizationsDetails.value.split(" ");
            }
            var immunizationsWithDate = {};
            _.each(immunizationsGiven, function (immunization) {
                if (immunizationsAndDateMap[immunization]) {
                    immunizationsWithDate[immunization] = _.find(r.doc.formInstance.form.fields,function (field) {
                        return field.name.toLowerCase() === immunizationsAndDateMap[immunization].toLowerCase();
                    }).value || _.find(r.doc.formInstance.form.fields,function (field) {
                        return field.name === 'submissionDate';
                    }).value;
                }
            });

            var vitaminDosesWithDate = {};
            _.each(vitaminDoses, function (dose) {
                if (immunizationsAndDateMap['vitamin_a' + dose]) {
                    vitaminDosesWithDate[dose] = _.find(r.doc.formInstance.form.fields,function (field) {
                        return field.name === immunizationsAndDateMap['vitamin_a' + dose];
                    }).value || _.find(r.doc.formInstance.form.fields,function (field) {
                        return field.name === 'submissionDate';
                    }).value;
                }
            });

            var childIdField = _.find(r.doc.formInstance.form.fields, function (field) {
                return field.name === 'childId';
            });

            if (childIdField === undefined) {
                immunizationsWithDate.entityId = _.find(r.doc.formInstance.form.fields,function (field) {
                    return field.name === 'id';
                }).value;
                vitaminDosesWithDate.entityId = _.find(r.doc.formInstance.form.fields,function (field) {
                    return field.name === 'id';
                }).value;
            } else {
                immunizationsWithDate.entityId = childIdField.value;
                vitaminDosesWithDate.entityId = childIdField.value;
            }

            return {immunizationsWithDate: immunizationsWithDate, vitaminDosesWithDate: vitaminDosesWithDate};
        });
    };


    var getChildImmunizationsInChildRegistrationOA = function () {
        var deferred = q.defer();
        var childIds = _.pluck(openChildren, 'value');

        formRepository.queryView('ChildRegistrationOA_FormSubmission_Temp/byFormName', {
            keys: childIds,
            include_docs: true
        }).then(function (response) {
            childRegistrationOAFormSubmissions = response;
            console.log('Found %s Child Registration OA.', response.length);
            deferred.resolve();
        }, function (error) {
            deferred.reject(error);
        });
        return deferred.promise;
    };

    var getMapOfImmunizationsAndEntityIdsFromChildRegistrationOA = function () {
        var deferred = q.defer();
        var childImmunizationAndVitaminDetails = getImmunizationsFromForm(childRegistrationOAFormSubmissions);
        allImmunizationsWithEntityIdAndDate.push(_.pluck(childImmunizationAndVitaminDetails, 'immunizationsWithDate'));
        allVitaminsWithEntityIdAndDate.push(_.pluck(childImmunizationAndVitaminDetails, 'vitaminDosesWithDate'));

        deferred.resolve();
        return deferred.promise;
    };

    var getChildImmunizationsInPNCRegistrationOA = function () {
        var deferred = q.defer();

        formRepository
            .queryView('PNCRegistrationOA_FormSubmission_Temp/byFormName',
            {
                include_docs: true
            })
            .then(function (response) {
                pncRegistrationOAFormSubmissions = response;
                console.log('Found %s Child PNC registration OAs.', response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getImmunizationsFromSubForms = function (formSubmissions) {
        return _.map(formSubmissions, function (r) {
            var submissionDate = _.find(r.doc.formInstance.form.fields,function (field) {
                return field.name === 'submissionDate';
            }).value;
            var immunizationsWithDate = [];
            _.each(r.doc.formInstance.form.sub_forms[0].instances, function (instance) {
                if (instance['immunizationsGiven'] != undefined) {
                    var immunizationsGiven = instance['immunizationsGiven'].split(" ");
                    var immunizationsForAChildWithDate = {};
                    _.each(immunizationsGiven, function (immunization) {
                        if (immunizationsAndDateMap[immunization] != undefined) {
                            immunizationsForAChildWithDate[immunization] = submissionDate;
                        }
                    });
                    immunizationsForAChildWithDate.entityId = instance['id'];
                    immunizationsWithDate.push(immunizationsForAChildWithDate);
                }
            });
            return immunizationsWithDate;
        });
    };

    var getMapOfImmunizationsAndEntityIdsFromPNCRegistrationOA = function () {
        var deferred = q.defer();
        var childImmunizations = getImmunizationsFromSubForms(pncRegistrationOAFormSubmissions);
        allImmunizationsWithEntityIdAndDate.push(_.flatten(childImmunizations));

        deferred.resolve();
        return deferred.promise;
    };

    var getChildImmunizationsInDeliveryOutcome = function () {
        var deferred = q.defer();

        formRepository
            .queryView('DeliveryOutcome_FormSubmission_Temp/byFormName',
            {
                include_docs: true
            })
            .then(function (response) {
                deliveryOutcomeFormSubmissions = response;
                console.log("Found %s Delivery outcome.", response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getMapOfImmunizationsAndEntityIdsFromDeliveryOutcome = function () {
        var deferred = q.defer();
        var childImmunizations = getImmunizationsFromSubForms(deliveryOutcomeFormSubmissions);
        allImmunizationsWithEntityIdAndDate.push(_.flatten(childImmunizations));

        deferred.resolve();
        return deferred.promise;
    };

    var getAllImmunizationsForChild = function () {
        var deferred = q.defer();
        var childIds = _.pluck(openChildren, 'value');
        formRepository.queryView('ChildImmunizations_FormSubmission_Temp/byFormName', {
            keys: childIds,
            include_docs: true
        }).then(function (response) {
            childImmunizationsFormSubmissions = response;
            console.log("Found %s Child Immunizations", response.length);
            deferred.resolve();
        }, function (error) {
            deferred.reject(error);
        });
        return deferred.promise;
    };

    var getMapOfImmunizationsAndEntityIdsFromChildImmunization = function () {
        var deferred = q.defer();
        var childImmunizations = _.map(childImmunizationsFormSubmissions, function (r) {
            var immunizationsGiven = _.find(r.doc.formInstance.form.fields,function (field) {
                return field.name === 'immunizationsGiven';
            }).value.split(" ");

            var previousImmunizations = _.find(r.doc.formInstance.form.fields,function (field) {
                return field.name === 'previousImmunizations';
            }).value.split(" ");

            var immunizationsDate = _.find(r.doc.formInstance.form.fields,function (field) {
                return field.name === 'immunizationDate';
            }).value;

            var immunizationsReceived = _.difference(immunizationsGiven, previousImmunizations);
            var immunizationsWithDate = {};
            _.each(immunizationsReceived, function (immunization) {
                immunizationsWithDate[immunization] = immunizationsDate;
            });

            immunizationsWithDate.entityId = r.doc.entityId;
            return immunizationsWithDate;
        });
        allImmunizationsWithEntityIdAndDate.push(childImmunizations);

        deferred.resolve();
        return deferred.promise;
    };

    var getAllVitaminsForChild = function () {
        var deferred = q.defer();
        var childIds = _.pluck(openChildren, 'value');
        formRepository
            .queryView('VitaminA_FormSubmission_Temp/byFormName',
            {
                keys: childIds,
                include_docs: true
            })
            .then(function (response) {
                vitaminAFormSubmissions = response;
                console.log("Found %s Vitamin A", response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getMapOfVitaminsAndEntityIdsFromVitaminInformation = function () {
        var deferred = q.defer();
        var childVitamins = _.map(vitaminAFormSubmissions, function (r) {
            var vitaminDose = _.find(r.doc.formInstance.form.fields,function (field) {
                return field.name === 'vitaminADose';
            }).value;
            var vitaminDate = _.find(r.doc.formInstance.form.fields,function (field) {
                return field.name === 'vitaminADate';
            }).value;

            var vitaminDosesWithDate = {};
            vitaminDosesWithDate[vitaminDose] = vitaminDate;
            vitaminDosesWithDate.entityId = r.doc.entityId;
            return vitaminDosesWithDate;
        });
        allVitaminsWithEntityIdAndDate.push(childVitamins);

        deferred.resolve();
        return deferred.promise;
    };

    var updateChildWithImmunizationInformation = function () {
        var deferred = q.defer();
        console.log("Immunizations: " + JSON.stringify(_.flatten(allImmunizationsWithEntityIdAndDate)));
        _.each(openChildren, function (child) {
            var allImmunizationsForChild = _.where(_.flatten(allImmunizationsWithEntityIdAndDate), {entityId: child.value});
            var uniqueImmunizations = {};
            _.each(allImmunizationsForChild, function (immunization) {
                _.each(_.keys(immunization), function (key) {
                    if (uniqueImmunizations[key] == undefined)
                        uniqueImmunizations[key] = immunization[key];
                    else if (new Date(uniqueImmunizations[key]) < new Date(immunization[key])) {
                        uniqueImmunizations[key] = immunization[key];
                    }
                });
                delete uniqueImmunizations.entityId;
            });

            child.doc.immunizations = uniqueImmunizations;
        });

        deferred.resolve();
        return deferred.promise;
    };

    var updateChildWithVitaminInformation = function () {
        var deferred = q.defer();
        console.log("Vitamins: " + JSON.stringify(_.flatten(allVitaminsWithEntityIdAndDate)));
        _.each(openChildren, function (child) {
            var allVitaminsForChild = _.where(_.flatten(allVitaminsWithEntityIdAndDate), {entityId: child.value});
            var uniqueVitamins = {};
            _.each(allVitaminsForChild, function (vitamin) {
                _.each(_.keys(vitamin), function (key) {
                    if (uniqueVitamins[key] == undefined)
                        uniqueVitamins[key] = vitamin[key];
                    else if (new Date(uniqueVitamins[key]) < new Date(vitamin[key])) {
                        uniqueVitamins[key] = vitamin[key];
                    }
                });
                delete uniqueVitamins.entityId;
            });

            child.doc.vitaminADoses = uniqueVitamins;
        });

        console.log("Updated document:" + JSON.stringify(openChildren));
        deferred.resolve();
        return deferred.promise;
    };


    var updateChildDocument = function () {
        var deferred = q.defer();
        var children = _.map(openChildren, function (child) {
            return child.doc;
        });
        dristhiRepository.save(children);
        deferred.resolve();
        return deferred.promise;
    };

    var deleteChild_TempView = function () {
        return dristhiRepository.deleteDocById('_design/Child_Temp');
    };

    var deleteChildImmunizationsFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/ChildImmunizations_FormSubmission_Temp');
    };

    var deleteChildRegistrationECFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/ChildRegistrationEC_FormSubmission_Temp');
    };

    var deleteChildRegistrationOAFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/ChildRegistrationOA_FormSubmission_Temp');
    };

    var deletePNCRegistrationOAFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/PNCRegistrationOA_FormSubmission_Temp');
    };

    var deleteDeliveryOutcomeFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/DeliveryOutcome_FormSubmission_Temp');
    };

    var deleteVitaminAFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/VitaminA_FormSubmission_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteChild_TempView(), deleteChildRegistrationECFormSubmission_TempView(), deleteChildRegistrationOAFormSubmission_TempView(), deleteChildImmunizationsFormSubmission_TempView(), deletePNCRegistrationOAFormSubmission_TempView(), deleteDeliveryOutcomeFormSubmission_TempView(), deleteVitaminAFormSubmission_TempView()]);
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
            .then(createChildRegistrationECFormSubmissionsView)
            .then(createChildRegistrationOAFormSubmissionsView)
            .then(createDeliveryOutcomeFormSubmissionsView)
            .then(createPNCRegistrationOAFormSubmissionsView)
            .then(createChildImmunizationsFormSubmissionsByNameView)
            .then(createVitaminAFormSubmissionsByNameView)
            .then(createAllOpenChildrenView)
            .then(getAllOpenChildren)
            .then(getChildImmunizationsInChildRegistrationEC)
            .then(getMapOfImmunizationsAndEntityIdsFromChildRegistrationEC)
            .then(getChildImmunizationsInChildRegistrationOA)
            .then(getMapOfImmunizationsAndEntityIdsFromChildRegistrationOA)
            .then(getChildImmunizationsInDeliveryOutcome)
            .then(getMapOfImmunizationsAndEntityIdsFromDeliveryOutcome)
            .then(getChildImmunizationsInPNCRegistrationOA)
            .then(getMapOfImmunizationsAndEntityIdsFromPNCRegistrationOA)
            .then(getAllImmunizationsForChild)
            .then(getMapOfImmunizationsAndEntityIdsFromChildImmunization)
            .then(getAllVitaminsForChild)
            .then(getMapOfVitaminsAndEntityIdsFromVitaminInformation)
            .then(updateChildWithImmunizationInformation)
            .then(updateChildWithVitaminInformation)
            .then(updateChildDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = ChildImmunizationsMigration;