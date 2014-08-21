var _ = require('underscore')._;
var q = require('q');
var cli = require('cli');

var DB_SERVER = cli.args[0];
var DB_PORT = cli.args[1];
var FORM_DB_NAME = cli.args[2];
var DRISTHI_DB_NAME = cli.args[3];
var FILE_NAME = cli.args[4];
var USERNAME = cli.args[5] || '';


var Migration = require('./migrations/' + FILE_NAME);
var migration = new Migration(_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME, USERNAME);

migration.migrate();