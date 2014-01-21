var q = require('q');
var cradle = require('cradle');

var Repository = function () {
    var connectToDB = function (host, port, name) {
        var connection = new cradle.Connection(host, port, {cache: true, raw: false}).database(name);
        console.log('Connected to DB: %s.', name);
        return connection;
    };

    return {
        connectToDB: function (host, port, name) {
            return connectToDB(host, port, name);
        }};
};

module.exports = Repository;