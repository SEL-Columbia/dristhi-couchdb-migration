var UserDeletionMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME, USER_NAME) {
    var formRepository;
    var dristhiRepository;
    var userName;
    var entities;
    var user;
    var formSubmissions;

    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
        formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
        dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
        userName = USER_NAME;
        deferred.resolve();
        return deferred.promise;
    };

    var createEntitiesForAnUserView = function () {
        return dristhiRepository.createView('_design/EntitiesForAnUser_Temp', {
                byANMId: {
                    map: function (doc) {
                        if (doc.type === 'EligibleCouple' || doc.type === 'Mother' || doc.type === 'Child' || doc.type === 'Action') {
                            emit(doc.anmIdentifier);
                        }
                    }
                }
            }
        );
    };

    var createFormSubmissionsForAnUserView = function () {
        return formRepository.createView('_design/FormSubmissionsForAnUser_Temp', {
                byANMId: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission') {
                            emit(doc.anmId);
                        }
                    }
                }
            }
        );
    };

    var createDristhiUserView = function () {
        return dristhiRepository.createView('_design/DristhiUser_Temp', {
                byANMId: {
                    map: function (doc) {
                        if (doc.type == 'DrishtiUser') {
                            emit(doc.username);
                        }
                    }
                }
            }
        );
    };

    var getEntitiesForAnUser = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('EntitiesForAnUser_Temp/byANMId', {
                include_docs: true,
                key: userName
            })
            .then(function (response) {
                entities = response;
                console.log("Found Entities: %s", response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };


    var getAllFormSubmissionsForAnUser = function () {
        var deferred = q.defer();
        formRepository
            .queryView('FormSubmissionsForAnUser_Temp/byANMId', {
                include_docs: true,
                key: userName
            })
            .then(function (response) {
                formSubmissions = response;
                console.log("Found : %s", response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var getDristhiUser= function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('DristhiUser_Temp/byANMId', {
                include_docs: true,
                key: userName
            })
            .then(function (response) {
                user = response;
                console.log("Found Entities: %s", response.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var deleteEntityDocuments = function () {
        var deferred = q.defer();
        var entityDocuments = _.map(entities, function (entity) {
            return entity.doc;
        });
        dristhiRepository.deleteDocs(entityDocuments);
        deferred.resolve();
        return deferred.promise;
    };

    var deleteFormSubmissionsDocuments = function () {
        var deferred = q.defer();
        var formSubmissionDocuments = _.map(formSubmissions, function (formSubmission) {
            return formSubmission.doc;
        });
        formRepository.deleteDocs(formSubmissionDocuments);
        deferred.resolve();
        return deferred.promise;
    };

    var deleteDristhiUserDocuments = function () {
        var deferred = q.defer();
        var userDocument = _.map(user, function (u) {
            return u.doc;
        });
        dristhiRepository.deleteDocs(userDocument);
        deferred.resolve();
        return deferred.promise;
    };

    var deleteFormSubmissionsForAnUser_TempView = function () {
        return formRepository.deleteDocById('_design/FormSubmissionsForAnUser_Temp');
    };

    var deleteEntitiesForAnUser_TempView = function () {
        return dristhiRepository.deleteDocById('_design/EntitiesForAnUser_Temp');
    };

    var deleteDristhiUser_TempView = function () {
        return dristhiRepository.deleteDocById('_design/DristhiUser_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteFormSubmissionsForAnUser_TempView(), deleteEntitiesForAnUser_TempView(), deleteDristhiUser_TempView()]);
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
            .then(createEntitiesForAnUserView)
            .then(createFormSubmissionsForAnUserView)
            .then(createDristhiUserView)
            .then(getEntitiesForAnUser)
            .then(getAllFormSubmissionsForAnUser)
            .then(getDristhiUser)
            .then(deleteEntityDocuments)
            .then(deleteFormSubmissionsDocuments)
            .then(deleteDristhiUserDocuments)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = UserDeletionMigration;