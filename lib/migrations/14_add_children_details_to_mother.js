var ChildrenDetailsMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var openPNCs;
    var childrenDetails = [];
    var formRepository;
    var dristhiRepository;
    var deliveryOutcomeFormSubmissions;
    var pncRegistrationOAFormSubmissions;


    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
        formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
        dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
        deferred.resolve();
        return deferred.promise;
    };

    var createDeliveryOutcomeFormSubmissionsView = function () {
        return formRepository.createView('_design/DeliveryOutcome_FormSubmission_Temp', {
                byFormName: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission' && doc.formName === 'delivery_outcome') {
                            emit(doc.instanceId, doc.entityId);
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
                            emit(doc.instanceId, doc.formInstance.form.fields[1].value);
                        }
                    }
                }
            }
        );
    };

    var createAllOpenPNCsView = function () {
        return dristhiRepository.createView('_design/Mother_Temp', {
                allOpenPNCs: {
                    map: function (doc) {
                        if (doc.type === 'Mother' && doc.isClosed === 'false' && doc.details.type === 'PNC') {
                            emit(doc._id, doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var getAllOpenPNCs = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('Mother_Temp/allOpenPNCs', {
                include_docs: true
            })
            .then(function (response) {
                openPNCs = response;
                console.log("Found open PnCs: %s", response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getAllDeliveryOutcomes = function () {
        var deferred = q.defer();
        formRepository
            .queryView('DeliveryOutcome_FormSubmission_Temp/byFormName',
            {
                include_docs: true
            })
            .then(function (response) {
                deliveryOutcomeFormSubmissions = response;
                console.log("Found Delivery outcome: %s", response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getAllPNCRegistrationOAs = function () {
        var deferred = q.defer();

        formRepository
            .queryView('PNCRegistrationOA_FormSubmission_Temp/byFormName',
            {
                include_docs: true
            })
            .then(function (response) {
                pncRegistrationOAFormSubmissions = response;
                console.log("Found PNC Registration OA: %s", response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getAllChildrenDetails = function () {
        childrenDetails.push(_.flatten(getChildrenDetailsFromSubForms(deliveryOutcomeFormSubmissions)));
        childrenDetails.push(_.flatten(getChildrenDetailsFromSubForms(pncRegistrationOAFormSubmissions)));
    };

    var getChildrenDetailsFromSubForms = function (formSubmissions) {
        return _.map(formSubmissions, function (r) {
            var deliveryOutcome = _.find(r.doc.formInstance.form.fields, function (field) {
                return field.name === 'deliveryOutcome';
            }).value;
            if (deliveryOutcome !== 'still_birth') {
                return _.map(r.doc.formInstance.form.sub_forms[0].instances, function (instance) {
                    return {
                        motherId: r.value,
                        id: instance.id,
                        gender: instance.gender,
                        weight: instance.weight,
                        immunizationsAtBirth: instance.immunizationsGiven
                    }
                });
            }
            else {
                return [];
            }
        });
    };

    var updateAllOpenPNCsWithChildrenDetails = function () {
        console.log("Children Details: %s", JSON.stringify(childrenDetails));
        _.each(openPNCs, function (pnc) {
            pnc.doc.childrenDetails = _.where(_.flatten(childrenDetails), {motherId: pnc.value});
            _.each(pnc.doc.childrenDetails, function (child) {
                delete child.motherId;
            })
        });
        console.log('Modified PNCs: %s', JSON.stringify(openPNCs));
    };

    var updateMotherDocument = function () {
        var deferred = q.defer();
        var mothers = _.map(openPNCs, function (mother) {
            return mother.doc;
        });
        dristhiRepository.save(mothers);
        deferred.resolve();
        return deferred.promise;
    };

    var deleteMother_TempView = function () {
        return dristhiRepository.deleteDocById('_design/Mother_Temp');
    };

    var deleteDeliveryOutcome_FormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/DeliveryOutcome_FormSubmission_Temp');
    };
    var deletePNCRegistrationOA_FormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/PNCRegistrationOA_FormSubmission_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteMother_TempView(), deleteDeliveryOutcome_FormSubmission_TempView(), deletePNCRegistrationOA_FormSubmission_TempView()]);
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
            .then(createDeliveryOutcomeFormSubmissionsView)
            .then(createPNCRegistrationOAFormSubmissionsView)
            .then(createAllOpenPNCsView)
            .then(getAllOpenPNCs)
            .then(getAllDeliveryOutcomes)
            .then(getAllPNCRegistrationOAs)
            .then(getAllChildrenDetails)
            .then(updateAllOpenPNCsWithChildrenDetails)
            .then(updateMotherDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = ChildrenDetailsMigration;