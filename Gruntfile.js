'use strict';
module.exports = function (grunt) {
    require('time-grunt')(grunt);
    require('load-grunt-tasks')(grunt);
    var target = grunt.option('target') || '0_help_file';

    grunt.initConfig({
        env: {
            dev: {
                port: 5984
            }
        },
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
            dev: {
                script: 'lib/index.js',
                options: {
                    args: ['http://localhost', 5984, 'drishti-form', 'drishti', target],
                    watch: ''
                }
            },
            ec2: {
                script: 'lib/index.js',
                options: {
                    args: ['http://localhost', 5985, 'drishti-form', 'drishti', target],
                    watch: ''
                }
            },
            prod: {
                script: 'lib/index.js',
                options: {
                    args: ['http://localhost', 5986, 'drishti-form', 'drishti', target],
                    watch: ''
                }
            }
        }
    });
    grunt.registerTask('default', ['help']);
    grunt.registerTask('migrate-dev', ['nodemon:dev']);
    grunt.registerTask('help', ['nodemon:help']);
};