var _ = require('underscore')._;
var q = require('q');
var cradle = require('cradle');

var Repository = function (host, port, name) {
    var connection = new cradle.Connection(host, port, {cache: true, raw: false}).database(name);
    console.log('Connected to DB: %s.', name);

    var createView = function (name, viewFunction) {
        var deferred = q.defer();
        connection.save(name,
            {views: viewFunction },
            function (err, res) {
                if (err) {
                    console.error('Error when creating view : %s. Message: %s.', name, err);
                    deferred.reject(err);
                }
                console.log('Created view: %s.', name);
                deferred.resolve(res);
            });
        return deferred.promise;
    };

    var queryView = function (name, options) {
        var deferred = q.defer();
        connection.view(name, options, function (err, response) {
            if (err) {
                console.error('Error when querying view: %s. Error %s.', name, JSON.stringify(err));
                deferred.reject(err);
            }
            deferred.resolve(response);
        });
        return deferred.promise;
    };

    var save = function (docs) {
        var deferred = q.defer();
        connection.save(docs, function (err, res) {
            if (err) {
                deferred.reject('Error when bulk updating ANCs: ' + JSON.stringify(err));
            }
            var notUpdatedDocs = _.filter(res, function (item) {
                return _.has(item, 'error');
            });
            if (notUpdatedDocs.length > 0) {
                deferred.reject('Unable to update following ANCs: ' + JSON.stringify(notUpdatedDocs));
            }
            console.log('Updated %s docs.', docs.length);
            deferred.resolve();
        });
        return deferred.promise;
    };

    return {
        connectToDB: function (host, port, name) {
            return connectToDB(host, port, name);
        },
        createView: function (name, viewFunction) {
            return createView(name, viewFunction);
        },
        queryView: function (name, options) {
            return queryView(name, options);
        },
        save: function (docs) {
            return save(docs);
        }
    };
};

module.exports = Repository;