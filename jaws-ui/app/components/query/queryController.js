angular.module('JawsUI.query.controller', [
		'ngMaterial',
		'ui.codemirror',
		'lrInfiniteScroll',
		'JawsUI.directives.showTail',
		'JawsUI.filters', 
		'JawsUI.constants',
		'JawsUI.config',
		'anguFixedHeaderTable'])
.controller('QueryCtrl', [
	'$scope',
	'$timeout',
	'$interval',
	'jawsService',
	'EVENTS',
	'QUERY_STATUS',
	'CONFIG',
	'$mdMedia',
	'$mdDialog',
	function($scope, $timeout, $interval, jawsService, EVENTS,QUERY_STATUS, CONFIG,$mdMedia,$mdDialog) {
		var TABIDX_HISTORY = 0;
		var TABIDX_LOGS = 1;
		var TABIDX_RESULTS = 2;
		var historyStartQueryId = '';
		var queriesHistoryLeft = true;
		var queryLogsTimeout;
		var displayLogsTimeout;
		var curQueryId = "";
		var queriesToMonitor = [];
		var lastLogStamp = 0;
		var resultsOffset = 0;
		var queryStatusCheckTimeout = $interval(checkQueriesStatus, 5000);
		
		$scope.$mdMedia = $mdMedia;
		$scope.queryLogsBusy = false;
		$scope.queryHistoryBusy = false;
		$scope.resultsBusy = false;
		$scope.resultsAvailable = false;
		$scope.resultsStorage = "HDFS";
		$scope.limitedQuery = true;
		$scope.logsText = "";
		$scope.editorOptions = {
			mode: 'sql',
			indentWithTabs: true,
			smartIndent: true,
			lineNumbers: true,
			matchBrackets: true,
			autofocus: true,
			extraKeys: {
				"Ctrl-Space": "autocomplete"
			},
			hintOptions: {
				tables: []
			}
		};
		$scope.data = {
			editorQuery: "",
			logs: [],
			queryResponse: {
				schema: [],
				results: []
			},
			queries: []
		};	
		
		$scope.$on(EVENTS.TABLES_UPDATED, function(e, tables) {
			//refresh table list for autocomplete
			var tablesObj = tables.reduce(function(o, v, i) {
				o[v] = '';
				return o;
			}, {});
			$scope.editorOptions.hintOptions.tables = tablesObj;

		});
		$scope.$on(EVENTS.COLUMNS_UPDATED, function(e, tbl) {
			//refresh table columns for autocomplete
			$scope.editorOptions.hintOptions.tables[tbl.name] = tbl.columns;
		});
		$scope.$on(EVENTS.DB_SWITCHED, function(e, db) {
			//replace current database in query editor
			var newVal = $scope.data.editorQuery.replace(/use\b\s*[\w]+;?/i, "");
			newVal = "use " + db + ";" + newVal;
			$scope.data.editorQuery = newVal;

		});
		$scope.$on('$locationChangeStart', function() {
			//avoid running checks after leaving page
			$interval.cancel(queryStatusCheckTimeout);
			$timeout.cancel(queryLogsTimeout);
			$timeout.cancel(displayLogsTimeout);
		});
		
		/**
		* switch to next tab on swipe
		*/
		$scope.next = function() {
			$scope.data.selectedIndex = Math.min($scope.data.selectedIndex + 1, 2);
		};
		
		/**
		* switch to previous tab on swipe
		*/
		$scope.previous = function() {
			$scope.data.selectedIndex = Math.max($scope.data.selectedIndex - 1, 0);
		};	
		
		function switchTab(newTabIdx) {
			$timeout(function() {
				$scope.data.selectedIndex = newTabIdx;
			},10);
		}
		
		/**
		* display a past query (logs + results)
		*/
		$scope.displayQuery = function(query) {
			clearQueryData();
			$scope.data.editorQuery = query.query;
			curQueryId = query.queryID;
			if (query.state === QUERY_STATUS.IN_PROGRESS) {
				switchTab(TABIDX_LOGS);
				waitForResults();
			}
			else {
				displayQueryLogs();
				if (query.state === QUERY_STATUS.DONE)  {
					displayQueryResults();
					switchTab(TABIDX_RESULTS);
				}
				else {
					switchTab(TABIDX_LOGS);
				}
			}
		};	
		$scope.clearQueryData =  clearQueryData;
		
		/**
		* deletes a query 
		*/
		$scope.deleteQuery = function(query) {
			var confirm = $mdDialog.confirm()
				.content('Are you sure you want to delete the selected query?')
				.ok('Yes')
				.cancel('No');
			$mdDialog.show(confirm).then(function() {  
				jawsService.deleteQuery(query.queryID)
					.then(function(response) {
						$scope.showConfirmation("Query deleted!");
						var idx = $scope.data.queries.indexOf(query);
						if (idx>=0)
							$scope.data.queries.splice(idx, 1);
					},function(response) {$scope.warnNetworkError();});
				});
			
		};
		
		/**
		* cancels an active query 
		*/
		$scope.cancelQuery = function(query) {
			if (query.state == QUERY_STATUS.IN_PROGRESS) {
				var confirm = $mdDialog.confirm()
					.content('Are you sure you want to cancel the selected query?')
					.ok('Yes')
					.cancel('No');
				$mdDialog.show(confirm).then(function() {  
					jawsService.cancelQuery(query.queryID)
						.then(function(response) {
							$scope.showConfirmation("Query cancelled!");
						},function(response) {$scope.warnNetworkError();});
					});
			} else {
				$scope.showError("Can't cancel a query that is not active");
			}
		};		
		
		
		/**
		* execute query typed in editor
		*/
		$scope.executeQuery = function() {
			var queryStr = $scope.data.editorQuery.trim();
			if (queryStr === "") {
				$scope.showError("Please type a query first!");
			}
			else {
				clearQueryData();
				
				jawsService.executeQuery(queryStr,
					$scope.limitedQuery,99,$scope.resultsStorage)
						.then(function(queryId) {
							$scope.showInfo("Query is running...");
							curQueryId = queryId;
							switchTab(TABIDX_LOGS);
							query = {
								"state": QUERY_STATUS.IN_PROGRESS,
								"queryID": queryId,
								"query": queryStr
							};
							$scope.data.queries.unshift(query);
							monitorQueryStatus(query);
							waitForResults();
						},function(response) {$scope.warnNetworkError();});
			}

		};
		
		/**
		* returns the next page of executed queries
		*/
		$scope.getNextQueryHistoryPage = function() {
			if (!$scope.queryHistoryBusy && queriesHistoryLeft) {
				$scope.queryHistoryBusy = true;
				jawsService.getQueryHistory(historyStartQueryId,CONFIG.QUERIES_PER_PAGE)
					.then(function(response) {
						queries = response.queries;
						queries.forEach(function(query) {
							$scope.data.queries.push(query);
							if (query.state===QUERY_STATUS.IN_PROGRESS) {	
								monitorQueryStatus(query);
							}
							historyStartQueryId = query.queryID;
						});
						
						if (queries.length < CONFIG.QUERIES_PER_PAGE) 
							queriesHistoryLeft = false;							
						
						$scope.queryHistoryBusy = false;
					},function(response) {$scope.warnNetworkError();});
			}
		};
		$scope.querySelected = function() {
			return curQueryId!=="";
		};
		
		/**
		* get the next page of results for current query
		*/
		$scope.getNextResultsPage = function() {
			if (curQueryId!=="" && !$scope.resultsBusy && resultsOffset >= 0) {
				$scope.resultsBusy = true;
				jawsService.getQueryResults(curQueryId,resultsOffset,CONFIG.RESULTS_PER_PAGE)
					.then(function(resultSet) {
						if (resultsOffset === 0)
							$scope.data.queryResponse.schema=resultSet.schema;
						resultSet.results.forEach(function(result) {
							$scope.data.queryResponse.results.push(result);
							resultsOffset++;
						});
						
						if (resultSet.results.length < CONFIG.RESULTS_PER_PAGE)
							resultsOffset = -1;

						$scope.resultsBusy = false;
					},function(response) {$scope.warnNetworkError();});
			}
		};	

		/**
		* updates the text for the logs textarea
		*/
		function updateLogsText() {
			var text = "";
			$scope.data.logs.forEach(function(log) {
				text += log.message+'\n';
			});
			$scope.logsText = text;
		}
		
		/**
		* clear data and timeouts for the current query
		*/
		function clearQueryData() {
			lastLogStamp = 0;
			$timeout.cancel(queryLogsTimeout);
			$timeout.cancel(displayLogsTimeout);
			curQueryId = "";
			$scope.resultsAvailable = false;
			$scope.queryLogsBusy = false;
			$scope.resultsBusy = false;
			$scope.data.logs = [];
			updateLogsText();
			$scope.data.queryResponse.schema = [];
			$scope.data.queryResponse.results = [];
		}
		
		/**
		* check if status for a query changed, and update if necessary
		* @param {Object} query the query object 
		*/
		function checkQueryStatus(query) {
			jawsService.getQueryInfo(query.queryID)
				.then(function(response) {
					if (response.state!==query.state)
						query.state=response.state;
				});
		}
		
		/**
		* check the status of monitored active queries
		*/
		function checkQueriesStatus() {
			for (i=queriesToMonitor.length-1;i>=0;i--) {
				var query = queriesToMonitor[i];
				if (query.state===QUERY_STATUS.IN_PROGRESS) {
					checkQueryStatus(query);
				}
				else {
					queriesToMonitor.splice(i,1);
				}
				
			}
		}
		
		/**
		* add a query to the list of monitored active queries
		* @param {Object} query the query object
		*/
		function monitorQueryStatus(query) {
			queriesToMonitor.push(query);
		}
		
		/**
		* Adds logs to the current list of logs
		* @param {Object[]} list of logs to add
		*/
		function addLogs(logs) {
			logs.forEach(function(log) {
				$scope.data.logs.push({
					id: $scope.data.logs.length,
					message: log.log
				});
				lastLogStamp = log.timestamp;
			});
			updateLogsText();
		}

		/**
		* Display logs for a query that is complete. 
		* The function uses a timer to repeatedly get pages of logs until all logs have been retrieved.
		*/
		function displayQueryLogs() {
			$scope.queryLogsBusy = true;
			displayLogsTimeout = $timeout(function() {
				jawsService.getQueryLogs(curQueryId,lastLogStamp,CONFIG.LOGS_PER_PAGE)
					.then(function(response) {
						addLogs(response.logs);
						if (response.logs.length >= CONFIG.LOGS_PER_PAGE) 
							displayQueryLogs();
						else
							$scope.queryLogsBusy = false;
					});
				},10);
			
		}

		/**
		* Wait & display new logs as soon as they are available for an active query
		*/
		function waitForResults() {
			$scope.queryLogsBusy = true;
			queryLogsTimeout = $timeout(function() {
				jawsService.getQueryLogs(curQueryId,lastLogStamp,CONFIG.LOGS_PER_PAGE)
					.then(function(response) {		
						addLogs(response.logs);
						switch (response.status) {
							case QUERY_STATUS.IN_PROGRESS:
								waitForResults();
								break;
							case QUERY_STATUS.DONE:
								$scope.showConfirmation("Query executed successfully");
								switchTab(TABIDX_RESULTS);
								displayQueryResults();
								//make sure we display remaining logs, if any
								if (response.logs.length >= CONFIG.LOGS_PER_PAGE)
									displayQueryLogs();
								else
									$scope.queryLogsBusy = false;
								break;
							case QUERY_STATUS.FAILED:
								$scope.queryLogsBusy = false;
								$scope.showError("Query failed!");
						}
					},function(response) {$scope.warnNetworkError();});
			}, 1000);
		}
		
		/**
		* returns first page of results for the current query
		*/
		function displayQueryResults() {
			$scope.resultsAvailable = true;
			resultsOffset = 0;
			$scope.getNextResultsPage();

		}
	}
]);