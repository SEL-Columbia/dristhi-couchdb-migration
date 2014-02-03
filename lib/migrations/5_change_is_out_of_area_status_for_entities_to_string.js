var ECOutOfAreaStatusMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var allEntities;
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

    var createAllECsView = function () {
        return dristhiRepository.createView('_design/All_ECs_Temp', {
                allECs: {
                    map: function (doc) {
                        if (doc.type === 'EligibleCouple') {
                            emit(doc, doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var getAllECs = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('All_ECs_Temp/allECs')
            .then(function (response) {
                allEntities = response;
                var entityIds = _.pluck(allEntities, 'value');
                console.log('Found ' + entityIds.length + ' Entities.');
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var updateOutOfAreaStatus = function () {
        var deferred = q.defer();
        _.each(allEntities, function (entity) {
            if (entity.key.isOutOfArea && entity.key.isOutOfArea.toString() === 'true') {
                entity.key.isOutOfArea = 'true';
            }
            else {
                entity.key.isOutOfArea = 'false';
            }
        });

        deferred.resolve();
        return deferred.promise;
    };

    var updateDocument = function () {
        var deferred = q.defer();
        var entities = _.map(allEntities, function (entity) {
            return entity.key;
        });
        dristhiRepository.save(entities);
        deferred.resolve();
        return deferred.promise;
    };

    var deleteAllEntities_TempView = function () {
        return dristhiRepository.deleteDocById('_design/All_ECs_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteAllEntities_TempView()]);
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
            .then(createAllECsView)
            .then(getAllECs)
            .then(updateOutOfAreaStatus)
            .then(updateDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = ECOutOfAreaStatusMigration;