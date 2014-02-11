var ChildANMIdentifierMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var formRepository, dristhiRepository;
    var childIdsWithANM = [];
    var children;

    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
        formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
        dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
        deferred.resolve();
        return deferred.promise;
    };

    var createPNCRegistrationOAView = function () {
        return formRepository.createView('_design/FormSubmission_Temp', {
                pncRegistrationOA: {
                    map: function (doc) {
                        if (doc.type === 'FormSubmission' && doc.formName === 'pnc_registration_oa') {
                            emit(doc.instanceId, [doc.formInstance.form.sub_forms[0].instances, doc.anmId]);
                        }
                    }
                }
            }
        );
    };

    var getChildIdsRegisteredUsingPNCRegistrationOAAndTheirANMId = function () {
        var deferred = q.defer();
        formRepository
            .queryView('FormSubmission_Temp/pncRegistrationOA')
            .then(function (response) {
                var childInstancesWithANMId = _.pluck(response, 'value');
                _.each(childInstancesWithANMId, function (instance) {
                    _.each(instance[0], function (child) {
                        childIdsWithANM.push({'childId': child['id'], 'anmIdentifier': instance[1]});
                    });
                });
                console.log('Found ' + JSON.stringify(childIdsWithANM) + ' instances registerd via pnc registration oa.');
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var createChildrenWithoutANMIdView = function () {
        return dristhiRepository.createView('_design/Child_Temp', {
                childWithoutANMId: {
                    map: function (doc) {
                        if (doc.type === 'Child' && !doc.anmIdentifier) {
                            emit(doc.caseId);
                        }
                    }
                }
            }
        );
    };

    var getAllChildWithoutANMId = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('Child_Temp/childWithoutANMId',
            {
                keys: _.pluck(childIdsWithANM, 'childId'),
                include_docs: true
            })
            .then(function (response) {
                children = response;
                console.log('Found %s Child.', children.length);
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var updateANMIdentifierDetail = function () {
        var deferred = q.defer();
        _.each(children, function (entity) {
            entity.doc.anmIdentifier = _.find(childIdsWithANM,function (child) {
                return child.childId === entity.doc.caseId;
            }).anmIdentifier;
        });
        console.log('Updated Children: ' + children);
        deferred.resolve();
        return deferred.promise;
    };

    var updateChildDocument = function () {
        var deferred = q.defer();
        var childrenDocument = _.map(children, function (child) {
            return child.doc;
        });
        dristhiRepository.save(childrenDocument);
        deferred.resolve();
        return deferred.promise;
    };


    var deleteFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/FormSubmission_Temp');
    };

    var deleteChild_TempView = function () {
        return dristhiRepository.deleteDocById('_design/Child_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([deleteFormSubmission_TempView(), deleteChild_TempView()]);
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
            .then(createPNCRegistrationOAView)
            .then(getChildIdsRegisteredUsingPNCRegistrationOAAndTheirANMId)
            .then(createChildrenWithoutANMIdView)
            .then(getAllChildWithoutANMId)
            .then(updateANMIdentifierDetail)
            .then(updateChildDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = ChildANMIdentifierMigration;