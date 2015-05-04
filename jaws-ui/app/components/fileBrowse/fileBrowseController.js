angular.module('JawsUI.fileBrowse.controller', [
			'JawsUI.promptDialog.controller',
			'JawsUI.fileBrowseService'])
.controller('FileBrowser', [
	'$scope',
	'$mdDialog',
	'$routeParams',
	'jawsService',
	'fileBrowseService',
	'CONFIG',
	
	function($scope, $mdDialog, $routeParams, jawsService, fileBrowseService, CONFIG) {
		$scope.files = [];
		$scope.currentDir = '/';
		$scope.loading = false;
		fileSystemLocation = "";
		//use filesystem provided in URL
		$scope.fileSystem = $routeParams.fileSystem;
		switch ($scope.fileSystem) {
			case "tachyon":
				fileSystemLocation = CONFIG.TACHYON_LOCATION;
				break;
			case "hdfs":
				fileSystemLocation = CONFIG.HDFS_LOCATION;
				break;
			default:
				$scope.showError("Unknown file system: "+$scope.fileSystem);
		}
		
		/**
		* Maps a directory to a parquet table 
		* @param {string} path - the full path of the dir to map.
		*/
		$scope.mapDir = function(dir) {
			mapFile(fileSystemLocation + dir);
		};

		/**
		* Sets the action to be taken when a file/folder is selected
		* @param {string} selectedFile - the name of the file/folder in the current directory
		*/
		$scope.selectFile = function(selectedFile) {

			if (selectedFile.isFolder) {
				if (selectedFile.name === '..')
					$scope.currentDir = $scope.currentDir.replace(/[^\/]+\/?$/, '');
				else
					$scope.currentDir += selectedFile.name + '/';
				refreshCurrentDir();
			} else {
				mapFile(fileSystemLocation + $scope.currentDir + selectedFile.name);
			}
		};
		
		/**
		* Maps a file/directory to a parquet table 
		* @param {string} path - the path of the file to map.
		*/
		function mapFile(path) {
			$mdDialog.show({
					controller: 'PromptDialog',
					templateUrl: 'components/promptDialog/prompt-dialog.html',
					locals: {
						prompt: "Enter the table name to map the file/dir to:",
						caption: "Caption",
						defaultValue: "parquet_table",
					}
				})
				.then(function(answer) {
					jawsService.registerParquetTable(path, answer)
						.then(function (){
							$scope.showConfirmation("Table \""+answer+"\" registered!");
						}, function (response) {
							$scope.showError("Table not registered: "+response.data);
						});
				});			
		}
		
		/**
		* Updates the contents of the current directory
		*/
		function refreshCurrentDir() {
			$scope.loading = true;
			$scope.files = [];
			fileBrowseService.getFilesInDir(fileSystemLocation + $scope.currentDir)
				.then(function(response) {
					results = response.data.files;
					$scope.currentDir = response.data.parentPath;
					if ($scope.currentDir.substr($scope.currentDir.length-1,1) !== '/')
						$scope.currentDir += '/';
					if ($scope.currentDir !== '/')
						results.unshift({
							name: '..',
							isFolder: true
						});
					$scope.files = results;
					$scope.loading = false;
				},function(response) {$scope.warnNetworkError();});
		}

		refreshCurrentDir();


	}
]);