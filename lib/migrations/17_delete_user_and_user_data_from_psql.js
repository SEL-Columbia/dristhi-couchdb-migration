var PSQLDeletionMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME, USER_NAME) {

    var client;
    var pg = require('pg');
    var userName = USER_NAME;


    var connectToDB = function () {
        var deferred = q.defer();
        var conString = "pg://postgres:password@localhost:" + DB_PORT + "/"+ DRISTHI_DB_NAME;

        client = new pg.Client(conString);

        client.connect();

        deferred.resolve();
        return deferred.promise;

    };

    var deleteANMDetailsFromANMReportAnnualTarget = function () {
        var deferred = q.defer();
        console.log("Deleting anm_report.annual_target");

        client.query("DELETE FROM anm_report.annual_target WHERE anmidentifier = " +
            "(SELECT id FROM anm_report.dim_anm WHERE anmidentifier='" + userName + "');", function (err, result) {
            if (err) {
                deferred.reject(err);
            }
            console.log("Response: " + JSON.stringify(result));
            deferred.resolve();
        });
        return deferred.promise;
    };

    var deleteANMDetailsFromANMReportANMReportData = function () {
        var deferred = q.defer();
        console.log("Deleting anm_report.anm_report_data");
        client.query("DELETE FROM anm_report.anm_report_data WHERE anmidentifier = " +
            "(SELECT id FROM anm_report.dim_anm WHERE anmidentifier='" + userName + "');", function (err, result) {
            if (err) {
                deferred.reject(err);
            }
            console.log("Response: " + JSON.stringify(result));
            deferred.resolve();
        });
        return deferred.promise;
    };

    var deleteANMDetailsFromANMReportDimANM = function () {
        var deferred = q.defer();
        console.log("Deleting anm_report.dim_anm");
        client.query("DELETE FROM anm_report.dim_anm WHERE anmidentifier ='" + userName + "';", function (err, result) {
            if (err) {
                deferred.reject(err);
            }
            console.log("Response: " + JSON.stringify(result));
            deferred.resolve();
        });
        return deferred.promise;
    };

    var deleteANMDetailsFromReportAnnualTarget = function () {
        var deferred = q.defer();
        console.log("Deleting report.annual_target");
        client.query("DELETE FROM report.annual_target WHERE service_provider = " +
            "(SELECT sp.ID FROM report.dim_service_provider sp, report.dim_anm a WHERE " +
            "sp.service_provider = a.id and anmIdentifier='" + userName + "');", function (err, result) {
            if (err) {
                deferred.reject(err);
            }
            console.log("Response: " + JSON.stringify(result));
            deferred.resolve();
        });
        return deferred.promise;
    };

    var deleteANMDetailsFromReportServiceProvided = function () {
        var deferred = q.defer();
        console.log("Deleting report_service_provided");
        client.query("DELETE FROM report.service_provided WHERE service_provider = " +
            "(SELECT sp.id FROM report.dim_service_provider sp, report.dim_anm a WHERE " +
            "sp.service_provider = a.id and anmidentifier='" + userName + "');", function (err, result) {
            if (err) {
                deferred.reject(err);
            }
            console.log("Response: " + JSON.stringify(result));
            deferred.resolve();
        });
        return deferred.promise;
    };

    var deleteANMDetailsFromReportDimServiceProvider = function () {
        var deferred = q.defer();
        console.log("Deleting report.dim_service_provider");
        client.query("DELETE FROM report.dim_service_provider WHERE service_provider = " +
            "(SELECT ID FROM report.dim_anm WHERE anmIdentifier='" + userName + "');", function (err, result) {
            if (err) {
                deferred.reject(err);
            }
            console.log("Response: " + JSON.stringify(result));
            deferred.resolve();
        });
        return deferred.promise;
    };

    var deleteANMDetailsFromReportDimANM = function () {
        var deferred = q.defer();
        console.log("Deleting report.dim_anm");
        client.query("DELETE from report.dim_anm WHERE anmidentifier ='" + userName + "';", function (err, result) {
            if (err) {
                deferred.reject(err);
            }
            console.log("Response: " + JSON.stringify(result));
            deferred.resolve();
        });
        return deferred.promise;
    };
    var closeDatabaseConnection = function () {
        var deferred = q.defer();

        query.on('end', client.end.bind(client));

        deferred.resolve();
        return deferred.promise;
    };

    var migrate = function () {
        connectToDB()
            .then(deleteANMDetailsFromANMReportAnnualTarget)
            .then(deleteANMDetailsFromANMReportANMReportData)
            .then(deleteANMDetailsFromANMReportDimANM)
            .then(deleteANMDetailsFromReportAnnualTarget)
            .then(deleteANMDetailsFromReportServiceProvided)
            .then(deleteANMDetailsFromReportDimServiceProvider)
            .then(deleteANMDetailsFromReportDimANM)
            .fin(closeDatabaseConnection);

    };

    return {
        migrate: migrate
    };
};

module.exports = PSQLDeletionMigration;