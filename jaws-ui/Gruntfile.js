module.exports = function(grunt) {
	grunt.loadNpmTasks('grunt-usemin');
	grunt.loadNpmTasks('grunt-contrib-clean');
	grunt.loadNpmTasks('grunt-contrib-jshint');
	grunt.loadNpmTasks('grunt-contrib-concat');
	grunt.loadNpmTasks('grunt-contrib-uglify');
	grunt.loadNpmTasks('grunt-contrib-cssmin');
	grunt.loadNpmTasks('grunt-contrib-copy');
	grunt.loadNpmTasks('grunt-processhtml');

	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),

		jshint: {
			beforeconcat: ['app/**/*.js'],
		},
		
		useminPrepare: {
			html: 'app/index.html',
			options: {
				dest: 'dist'
			}
		},

		usemin: {
			html: ['dist/{,*/}*.html']
		},

		processhtml: {
			options: {
				commentMarker: 'process'
			},
			release: {
				files: {
					'dist/index.html': ['dist/index.html']
				}
			}
		},
		
		uglify: {
			options: {
				mangle: true,			
			},		
		},

		copy: {
			// copy:release copies all html and image files to dist preserving the structure
			// and all dependant libs that are already minified
			release: {
				files: [{
					expand: true,
					cwd: 'app',
					src: [
						'img/**/*.{png,gif,jpg,svg}',
						'**/*.html',
						'config.js'
					],
					dest: 'dist'
				},{
					expand: true,
					flatten: true,
					src:['bower_components/angular-material/angular-material.min.css',
						 'bower_components/bootstrap/dist/css/bootstrap.min.css'],
					dest:'dist/css/'
				},{
					expand: true,
					flatten: true,
					src:[
						'bower_components/jquery/dist/jquery.min.js'],
					dest:'dist/lib/'
				}]
			}
		},

		concat: {
			//concat already minified libs that are split in multiple files
			libs: {
				files: {
					'dist/lib/angular-material-full.min.js': [
						'bower_components/angular/angular.min.js',
						'bower_components/angular-route/angular-route.min.js',
						'bower_components/angular-animate/angular-animate.min.js',
						'bower_components/angular-aria/angular-aria.min.js',
						'bower_components/angular-material/angular-material.min.js'
						
					]
				}
			}
		},

		clean: {
			release: ["dist/*"], //clean dist dir before build
			tmp: [".tmp/*"] //clean tmp dir after build
		}
	});

	grunt.registerTask('build', [
		'jshint',
		'clean:release',
		'copy:release',
		'concat:libs',
		'useminPrepare',
		'concat:generated',
		'cssmin:generated',
		'uglify:generated',
		'usemin',
		'processhtml:release',
		'clean:tmp'
	]);
};