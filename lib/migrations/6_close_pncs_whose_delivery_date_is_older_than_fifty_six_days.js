var PNCTypeMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var mothersToBeUpdated;
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

    var createAllPNCsWhoseDeliveryDateIsGreaterThanFiftySixDaysView = function () {
        return dristhiRepository.createView('_design/Mother_Temp', {
                allOpenMothersWhoseDeliveryDateIsGreaterThanFiftySixDays: {
                    map: function (doc) {
                        if (doc.type === 'Mother' && doc.isClosed === 'false' &&
                            doc.details.type === 'PNC' &&
                            (Math.floor((new Date() - new Date(doc.referenceDate)) / (1000 * 3600 * 24)) >= 56)) {
                            emit(doc.caseId, doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var getAllMothersWhoseDeliveryDateIsGreaterThanFiftySixDays = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('Mother_Temp/allOpenMothersWhoseDeliveryDateIsGreaterThanFiftySixDays', {
                include_docs: true
            })
            .then(function (response) {
                mothersToBeUpdated = response;
                console.log('Found ' + mothersToBeUpdated.length + ' open mothers to be updated.');
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var closeMothers = function () {
        _.each(mothersToBeUpdated, function (pnc) {
            pnc.doc.isClosed = 'true';
        });
        var deferred = q.defer();
        deferred.resolve();
        return deferred.promise;

    };

    var updateMotherDocument = function () {
        var pncs = _.map(mothersToBeUpdated, function (pnc) {
            return pnc.doc;
        });
        console.log('Number of mothers to be updated, %s', pncs.length);
        console.log('All mothers to be updated, %s', JSON.stringify(pncs));
        return dristhiRepository.save(pncs);
    };

    var deleteMother_TempView = function () {
        return dristhiRepository.deleteDocById('_design/Mother_Temp');
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
            .then(createAllPNCsWhoseDeliveryDateIsGreaterThanFiftySixDaysView)
            .then(getAllMothersWhoseDeliveryDateIsGreaterThanFiftySixDays)
            .then(closeMothers)
            .then(updateMotherDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(deleteMother_TempView);
    };

    return {
        migrate: migrate
    };
};

module.exports = PNCTypeMigration;