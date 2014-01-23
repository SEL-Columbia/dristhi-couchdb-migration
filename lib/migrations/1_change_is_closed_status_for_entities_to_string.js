var EntityClosedStatusMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
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

    var createAllEntitiesView = function () {
        return dristhiRepository.createView('_design/All_Entities_Temp', {
                allEntities: {
                    map: function (doc) {
                        if (doc.type === 'Mother' || doc.type === 'EligibleCouple' || doc.type === 'Child') {
                            emit(doc, doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var getAllEntities = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('All_Entities_Temp/allEntities')
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

    var updateClosedStatus = function () {
        var deferred = q.defer();
//        console.log("All entities: "+JSON.stringify(allEntities));
        _.each(allEntities, function (entity) {
            if (entity.key.isClosed && entity.key.isClosed.toString() === 'true') {
                entity.key.isClosed = 'true';
            }
            else {
                entity.key.isClosed = 'false';
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
        return dristhiRepository.deleteDocById('_design/All_Entities_Temp');
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
            .then(createAllEntitiesView)
            .then(getAllEntities)
            .then(updateClosedStatus)
            .then(updateDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = EntityClosedStatusMigration;