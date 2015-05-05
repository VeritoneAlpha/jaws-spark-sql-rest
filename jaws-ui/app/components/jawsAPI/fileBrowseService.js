/**
* Service interacting with the File Browser API.
*/
angular.module('JawsUI.fileBrowseService', [
	'JawsUI.constants',
	'JawsUI.config'])
.service("fileBrowseService",[
		'$http',
		'$q',
		'CONFIG',
		function($http, $q, CONFIG) {
		
			//public API
			return ({
				getFilesInDir:getFilesInDir,
			});		

			/**
			* return a list of files in a given directory
			* @param {string} dir the path of the directory to list
			* @return {HttpPromise} a promise object
			*/
			function getFilesInDir(dir) {
				return getRequest('listFiles?routePath='+escape(dir));
			}
			
			/**
			* perform a HTTP GET request to the API
			* @param {string} method the method to execute
			* @param {string} [config] custom config for the HTTP request 
			* @return {HttpPromise} a promise object
			*/
			function getRequest(method,config) {
				return $http.get(CONFIG.FILEBROWSE_API_PATH+method+'&_r='+Math.random(),config);
			}
			
			/**
			* perform a HTTP POST request to the API
			* @param {string} method the method to execute
			* @param {string} data the data to post
			* @param {string} [config] custom config for the HTTP request 
			* @return {HttpPromise} a promise object
			*/
			function postRequest(method,data,config) {
				return $http.post(CONFIG.FILEBROWSE_API_PATH+method+'&_r='+Math.random(),data,config);
			}
		}
]);