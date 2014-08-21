'use strict';
module.exports = function (grunt) {
    require('time-grunt')(grunt);
    require('load-grunt-tasks')(grunt);
    grunt.loadNpmTasks('grunt-prompt');
    var target = grunt.option('target') || '0_help_file';
    var username = grunt.option('username') || {};
    var environment = grunt.option('env') || 'dev';
    var port = grunt.option('port') || '5984';

    grunt.initConfig({
        jshint: {
            options: {
                jshintrc: '.jshintrc',
                reporter: require('jshint-stylish')
            },
            gruntfile: {
                src: 'Gruntfile.js'
            },
            lib: {
                src: ['lib/**/*.js']
            }
        },
        nodemon: {
            help: {
                script: 'lib/index.js',
                options: {
                    args: ['', '', '', '', '0_help_file'],
                    watch: ''
                }
            },
            prod: {
                script: 'lib/index.js',
                options: {
                    args: ['http://localhost', port, 'drishti-form', 'drishti', target, username],
                    watch: ''
                }
            }
        },
        prompt: {
            migration: {
                options: {
                    questions: [
                        {
                            config: 'prompt',
                            type: 'input',
                            message: 'Are you sure that you want to delete the ' + JSON.stringify(username) + ' user?',
                            default: 'no'
                        }
                    ],
                    then: function () {
                        var config = grunt.config('prompt');
                        if ((config.toLowerCase() === 'y' || config.toLowerCase() === 'yes') && JSON.stringify(username) !== '{}')
                            grunt.task.run('nodemon:prod');
                        return true;
                    }
                },
                script: 'lib/index.js'
            }
        }
    });

    grunt.registerTask('default', ['help']);
    grunt.registerTask('build', ['jshint']);
    grunt.registerTask('migrate-dev', ['nodemon:dev']);
    grunt.registerTask('help', ['nodemon:help']);
    grunt.registerTask('delete-user', function () {
        grunt.task.run('prompt:migration');
    });
};