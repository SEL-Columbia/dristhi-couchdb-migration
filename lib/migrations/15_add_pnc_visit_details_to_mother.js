var PNCVisitsMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var openPNCs;
    var formRepository;
    var dristhiRepository;
    var pncVisitFormSubmissions;

    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
        formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
        dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
        deferred.resolve();
        return deferred.promise;
    };

    var createPNCVisitFormSubmissionsView = function () {
        return formRepository.createView('_design/PNCVisit_FormSubmission_Temp', {
                byFormName: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission' && doc.formName === 'pnc_visit') {
                            emit(doc.instanceId, doc.entityId);
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

    var getAllPNCVisits = function () {
        var deferred = q.defer();

        formRepository.queryView('PNCVisit_FormSubmission_Temp/byFormName',
            {
                include_docs: true
            })
            .then(function (response) {
                pncVisitFormSubmissions = response;
                console.log("Found PNC Visit: %s", response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var updatePNCVisitDetailsToMother = function () {
        var deferred = q.defer();
        var pncVisits = _.map(pncVisitFormSubmissions, function (r) {
            var fields = _.filter(r.doc.formInstance.form.fields, function (field) {
                return field.name === 'pncVisitDate' ||
                    field.name === 'pncVisitPerson' ||
                    field.name === 'pncVisitPlace' ||
                    field.name === 'difficulties1' ||
                    field.name === 'abdominalProblems' ||
                    field.name === 'vaginalProblems' ||
                    field.name === 'difficulties2' ||
                    field.name === 'breastProblems';
            });
            var pncVisitDetails = {
                entityId: r.doc.entityId,
                date: _.find(fields, function (field) {
                    return field.name === 'pncVisitDate';
                }).value || '',
                person: _.find(fields, function (field) {
                    return field.name === 'pncVisitPerson';
                }).value || '',
                place: _.find(fields, function (field) {
                    return field.name === 'pncVisitPlace';
                }).value || '',
                difficulties: _.find(fields, function (field) {
                    return field.name === 'difficulties1';
                }).value || '',
                abdominalProblems: _.find(fields, function (field) {
                    return field.name === 'abdominalProblems';
                }).value || '',
                vaginalProblems: _.find(fields, function (field) {
                    return field.name === 'vaginalProblems';
                }).value || '',
                urinalProblems: _.find(fields, function (field) {
                    return field.name === 'difficulties2';
                }).value || '',
                breastProblems: _.find(fields, function (field) {
                    return field.name === 'breastProblems';
                }).value || '',
                childrenDetails: _.flatten(getChildrenDetailsFromSubForms(r.doc.formInstance.form.sub_forms[0])) || []
            };
            _.each(_.keys(pncVisitDetails), function (key) {
                if (pncVisitDetails[key] === '') {
                    delete pncVisitDetails[key];
                }
            });
            return pncVisitDetails;
        });

        _.each(openPNCs, function (pnc) {
            var allPNCVisitsForMother = _.where(pncVisits, {entityId: pnc.value});
            _.each(allPNCVisitsForMother, function (pncVisit) {
                delete pncVisit.entityId;
            });
            pnc.doc.pncVisits = allPNCVisitsForMother;
            console.log('Added %s PNC Visits to PNC %s.', allPNCVisitsForMother.length, pnc.value);
        });
        console.log("Modified PNCs: %s" ,openPNCs);
        deferred.resolve();
        return deferred.promise;
    };

    var getChildrenDetailsFromSubForms = function (subForms) {
        return _.map(subForms.instances, function (instance) {
            return {
                id: instance.id,
                urineStoolProblems: instance.urineStoolProblems,
                activityProblems: instance.activityProblems,
                breathingProblems: instance.breathingProblems,
                skinProblems: instance.skinProblems
            }
        });
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

    var deletePNCVisit_FormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/PNCVisit_FormSubmission_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteMother_TempView(), deletePNCVisit_FormSubmission_TempView()]);
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
            .then(createPNCVisitFormSubmissionsView)
            .then(createAllOpenPNCsView)
            .then(getAllOpenPNCs)
            .then(getAllPNCVisits)
            .then(updatePNCVisitDetailsToMother)
            .then(updateMotherDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = PNCVisitsMigration;