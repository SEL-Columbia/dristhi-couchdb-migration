var FPChangeMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var formRepository, dristhiRepository, couples, forms, couplesToBeUpdated = [];

    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
        formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
        dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
        deferred.resolve();
        return deferred.promise;
    };

    var createAllOpenECsWhoChangedFPMethodView = function () {
        return dristhiRepository.createView('_design/EC_Temp', {
                allOpenECsWhoChangedFPMethod: {
                    map: function (doc) {
                        if (doc.type === 'EligibleCouple' &&
                            doc.isClosed === 'false' &&
                            doc.isOutOfArea === 'false' &&
                            doc.details.newMethod) {
                            emit(doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var createFPChangeANCRegistrationPPFPFormByECIdView = function () {
        return formRepository.createView('_design/FormSubmission_Temp', {
                fpChangeANCRegistrationPPFPFormByECId: {
                    map: function (doc) {
                        var entityId = doc.entityId;
                        if (doc.formName === 'postpartum_family_planning') {
                            entityId = doc.formInstance.form.fields[1].value;
                        }
                        if (doc.type === 'FormSubmission' &&
                            (doc.formName === 'fp_change' ||
                                doc.formName === 'anc_registration' ||
                                doc.formName === 'postpartum_family_planning')) {
                            emit(entityId, [doc.serverVersion, doc.formName]);
                        }
                    }
                }
            }
        );
    };

    var getAllOpenECsWhoChangedFPMethod = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('EC_Temp/allOpenECsWhoChangedFPMethod',
            {
                include_docs: true
            })
            .then(function (response) {
                couples = response;
                console.log('Found %s ECs.', couples.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getFPChangeANCRegistrationPPFPFormByECId = function () {
        var deferred = q.defer();
        formRepository
            .queryView('FormSubmission_Temp/fpChangeANCRegistrationPPFPFormByECId',
            {
                keys: _.pluck(couples, 'key')
            })
            .then(function (response) {
                forms = _.sortBy(response, function (form) {
                    return form.value[0];
                });
                console.log('Found %s Forms.', forms.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var findECsWhoseLastServiceWasFPChange = function () {
        var deferred = q.defer();
        deferred.resolve();
        _.each(couples, function (couple) {
            if (_.last(_.filter(forms, function (form) {
                return form.key === couple.doc.caseId;
            })).value[1] === 'fp_change') {
                couple.doc.details.currentMethod = couple.doc.details.newMethod;
                couplesToBeUpdated.push(couple.doc);
            }
        });
        console.log('%s number of couples to be updated.', couplesToBeUpdated.length);
        return deferred.promise;
    };

    var updateECDocument = function () {
        var deferred = q.defer();
        dristhiRepository.save(couplesToBeUpdated);
        deferred.resolve();
        return deferred.promise;
    };

    var deleteFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/FormSubmission_Temp');
    };

    var deleteEC_TempView = function () {
        return dristhiRepository.deleteDocById('_design/EC_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteFormSubmission_TempView(), deleteEC_TempView()]);
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
            .then(createAllOpenECsWhoChangedFPMethodView)
            .then(createFPChangeANCRegistrationPPFPFormByECIdView)
            .then(getAllOpenECsWhoChangedFPMethod)
            .then(getFPChangeANCRegistrationPPFPFormByECId)
            .then(findECsWhoseLastServiceWasFPChange)
            .then(updateECDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = FPChangeMigration;