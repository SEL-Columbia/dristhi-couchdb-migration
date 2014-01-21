var q = require('q');
var cradle = require('cradle');

var Repository = function () {
    var connectToDB = function (host, port, name) {
        var connection = new cradle.Connection(host, port, {cache: true, raw: false}).database(name);
        console.log('Connected to DB: %s.', name);
        return connection;
    };

    var createView = function (connection, name, viewFunction) {
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

    return {
        connectToDB: function (host, port, name) {
            return connectToDB(host, port, name);
        },
        createView: function (connection, name, viewFunction) {
            return createView(connection, name, viewFunction);
        }
    };
};

module.exports = Repository;