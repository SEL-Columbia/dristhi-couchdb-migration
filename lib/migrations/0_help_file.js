var HelpMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var migrate = function() {
        console.log('grunt: missing target');
        console.log("Usage: grunt nodemon:<env> --target=<file_name>");
        console.log("");
        console.log("Try 'grunt help' to get the help message");
    };

    return {
        migrate: function () {
            return migrate();
        }
    };
}

module.exports = HelpMigration;