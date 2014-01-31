/* jshint unused: false */

var HelpMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var migrate = function () {
        console.error('grunt: missing target');
        console.info("Usage: grunt nodemon:<env> --target=<file_name>");
        console.info("");
        console.info("Try 'grunt help' to get the help message");
    };

    return {
        migrate: function () {
            return migrate();
        }
    };
};

module.exports = HelpMigration;