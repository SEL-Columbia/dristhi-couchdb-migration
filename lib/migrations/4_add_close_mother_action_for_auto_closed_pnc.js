var AutoClosePNCMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var formRepository, dristhiRepository, pregnancyRegistrations, closeActions;

    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
        formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
        dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
        deferred.resolve();
        return deferred.promise;
    };

    var createDeliveryOutcomeDoneBeforeFiftySixDaysFromTodayView = function () {
        return formRepository.createView('_design/FormSubmission_Temp', {
                deliveryOutcomeDoneBeforeFiftySixDaysFromToday: {
                    map: function (doc) {
                        var field = doc.formInstance.form.fields.filter(function (field) {
                            return field.name === 'referenceDate';
                        })[1];
                        if (doc.type === 'FormSubmission' &&
                            doc.formName === 'delivery_outcome' &&
                            (Math.floor((new Date() - new Date(field.value)) / (1000 * 3600 * 24)) >= 56)) {
                            emit(doc.anmId, [doc.entityId, field.value]);
                        }
                    }
                }
            }
        );
    };

    var createPNCRegistrationOADoneBeforeFiftySixDaysFromTodayView = function () {
        return formRepository.createView('_design/FormSubmission_Temp1', {
                pncRegistrationOADoneBeforeFiftySixDaysFromToday: {
                    map: function (doc) {
                        var referenceDateField = doc.formInstance.form.fields.filter(function (field) {
                            return field.name === 'referenceDate';
                        })[0];
                        var motherIdField = doc.formInstance.form.fields.filter(function (field) {
                            return field.name === 'motherId';
                        })[0];
                        if (doc.type === 'FormSubmission' &&
                            doc.formName === 'pnc_registration_oa' &&
                            (Math.floor((new Date() - new Date(referenceDateField.value)) / (1000 * 3600 * 24)) >= 56)) {
                            emit(doc.anmId, [motherIdField.value, referenceDateField.value]);
                        }
                    }
                }
            }
        );
    };

    var getPregnancyRegistrations = function () {
        var deferred = q.defer();
        formRepository
            .queryView('FormSubmission_Temp/deliveryOutcomeDoneBeforeFiftySixDaysFromToday', {
                include_docs: false
            })
            .then(function (deliveryOutcomeResponse) {
                pregnancyRegistrations = deliveryOutcomeResponse;
                console.log('Found ' + deliveryOutcomeResponse.length + ' Delivery outcomes.');

                formRepository
                    .queryView('FormSubmission_Temp1/pncRegistrationOADoneBeforeFiftySixDaysFromToday', {
                        include_docs: false
                    })
                    .then(function (pncRegistrationOAResponse) {
                        pregnancyRegistrations = pregnancyRegistrations.concat(pncRegistrationOAResponse);
                        console.log('Found ' + pncRegistrationOAResponse.length + ' Pregnancy registrations.');
                        deferred.resolve();
                    }, function (error) {
                        deferred.reject(error);
                    });
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var createCloseMotherActions = function () {
        closeActions = [];
        console.log('Pregnancy registrations: %s', JSON.stringify(pregnancyRegistrations));
        _.each(pregnancyRegistrations, function (registration) {
            var timeStamp = new Date().getTime();
            closeActions.push({
                "type": "Action",
                "anmIdentifier": registration.key,
                "caseID": registration.value[0],
                "data": {
                    "reasonForClose": "Auto Close PNC"
                },
                "actionTarget": "mother",
                "actionType": "close",
                "isActionActive": true,
                "timeStamp": timeStamp,
                "details": {
                }
            });
            console.log('Created mother close action for mother: %s, anm: %s and timeStamp: %s',
                registration.value[0], registration.key, timeStamp);
        });
        var deferred = q.defer();
        deferred.resolve();
        return deferred.promise;
    };

    var saveCloseMotherActions = function () {
        return dristhiRepository.save(closeActions);
    };

    var deleteFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/FormSubmission_Temp');
    };

    var deleteFormSubmission_Temp1View = function () {
        return formRepository.deleteDocById('_design/FormSubmission_Temp1');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteFormSubmission_TempView(), deleteFormSubmission_Temp1View()]);
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
            .then(createDeliveryOutcomeDoneBeforeFiftySixDaysFromTodayView)
            .then(createPNCRegistrationOADoneBeforeFiftySixDaysFromTodayView)
            .then(getPregnancyRegistrations)
            .then(createCloseMotherActions)
            .then(saveCloseMotherActions)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = AutoClosePNCMigration;