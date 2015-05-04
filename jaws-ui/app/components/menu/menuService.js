
angular.module('JawsUI.menuService', [
	'JawsUI.jawsService',
	'JawsUI.constants',
	'JawsUI.config'])
.factory('menu', [
	'$location',
	'$rootScope',
	'jawsService',
	'EVENTS',
	function($location, $rootScope,jawsService,EVENTS) {

		var sections = [];	

		/**
		 * Populate children of section dynamically
		 * @callback populateCallback
		 * @param {Object} name of the section to populate children for
		 */
				
		
		/**
		* Creates a menu section that can be toggled (has children)
		* @param {string} sectionName the name of the new section
		* @param {Object} parentSection the parent menu section
		* @param {string} cssClass if provided, sets a CSS class for the section
		* @param {populateCallback} populateCallback function to call to populate children dynamically
		* @return {Object} the created toggle section
		*/		
		function createToggleSection(sectionName,parentSection,cssClass,populateCallback) {
			var section = {
					name: sectionName,
					parent: parentSection,
					type: 'toggle',
					class: cssClass,
					pages: []
			};
			if (populateCallback) {
				section.populateSubsections=populateCallback(section);
			}
			return section;
		}
		
		/**
		* add clickable links to a section
		* @param {string[]} list of links to add
		* @param {string} path path to navigate to
		* @return  {Object[]} list of clickable links
		*/
		function addSectionLinks(list,path) {
			var pages = [];
			for (var key in list) {
				var page = list[key];
				pages.push( {
					name: page,
					type: 'link',
					url: path
				});
			}
			return pages;
		}
		
		/**
		* Add a new database (toggle) section
		* @param {string} page name of the database
		* @param {Object} parentSection the parent menu section
		* @return {Object} menu section for the given database
		*/
		function addDBSection(page,parentSection) {
			return createToggleSection(page,parentSection,'database',
				function (subsection) {
					return function() { 
						subsection.loading = true;
						$rootScope.$broadcast(EVENTS.DB_SWITCHED,subsection.name);
						jawsService.getDBTables(subsection.name)
							.then(function (tables) {
								subsection.pages = addTableSections(tables,subsection); 
								$rootScope.$broadcast(EVENTS.TABLES_UPDATED,tables);
								subsection.loading = false;
						});
					};
				});
		}
		
		/**
		* Create a list of menu sections from a list of database names
		* @param {Object[]} list the list of database names
		* @param {Object} parentSection the name of the parent menu section
		* @return {Object[]} the created menu sections
		*/
		function addDBSections(list,parentSection) {
			var pages = [];
			for (var key in list) {
				var page = list[key];
				pages.push(addDBSection(page,parentSection));
			}
			return pages;
		}

		/**
		* Creates a menu section for a given table name
		* @param {string} page the name of the table
		* @param {Object} parentSection the name of the parent menu section
		* @return {Object} menu section for the given table 
		*/
		function addTableSection(page,parentSection) {
			return createToggleSection(page,parentSection,'table',
				function (subsection) {	
					return function() { 
						subsection.loading = true;
						var tableName = subsection.name;
						var dbName = subsection.parent.name; 
						jawsService.getTableColumns(dbName,tableName)
							.then(function (columns) {
								subsection.pages = addSectionLinks(columns,''); 
								$rootScope.$broadcast(EVENTS.COLUMNS_UPDATED,{"name":tableName, "columns": columns});
								subsection.loading = false;
						});
					};
				});
		}

		/**
		* Create a list of menu sections from a list of table names
		* @param {Object[]} list the list of table names
		* @param {Object} parentSection the name of the parent menu section
		* @return {Object[]} the created menu sections
		*/
		function addTableSections(list,parentSection) {
			var pages = [];
			for (var key in list) {
				var page = list[key];
				pages.push(addTableSection(page,parentSection));
			}
			return pages;
		}

		var querySections = [];
		var runQuerySection = {
			name: 'RUN QUERY',
			type: 'toggle',
			url: '/query',
			title: 'Run Query',
			pages: querySections
		};
				
		var dbSection = createToggleSection('DATABASES',runQuerySection,'',
			function (subsection) {	
				return function() {
					subsection.loading = true;
					jawsService.getDatabases()
						.then(function (dbs) {
							subsection.pages = addDBSections(dbs,subsection); 
							subsection.loading = false;
						});
				};
			});
		querySections.push(dbSection);
		
		querySections.push();					

		/**
		* Creates a menu section for a given parquet table name
		* @param {string} page the name of the parquet table
		* @param {Object} parentSection the name of the parent menu section
		* @return {Object} menu section for the given table 
		*/
		function addParquetTablesSection(page,parentSection) {
			var section = createToggleSection(page,parentSection,'table',
				function (subsection) {	
					return function() { 
						subsection.loading = true;
						var tableName=subsection.name;
						jawsService.getParquetTableColumns(tableName)
							.then(function (columns) {
								subsection.pages = addSectionLinks(columns,''); 
								$rootScope.$broadcast(EVENTS.COLUMNS_UPDATED,{"name":tableName, "columns": columns});
								subsection.loading = false;
						});
					};
				});
			section.removable = true;
			section.contentType = 'ptable';
			return section;
		}
		
		/**
		* Create a list of menu sections from a list of table names
		* @param {Object[]} list the list of table names
		* @param {Object} parentSection the name of the parent menu section
		* @return {Object[]} the created menu sections
		*/
		function addParquetTablesSections(list,parentSection) {
			var pages = [];
			for (var key in list) {
				var page = list[key];
				pages.push(addParquetTablesSection(page,parentSection));
			}
			return pages;
		}	

		var parquetSection = createToggleSection('PARQUET TABLES',runQuerySection,'',	
			function (subsection) {	
				return function() {
					subsection.loading = true;
					jawsService.getParquetTables()
						.then(function (tables) {
							
							subsection.pages = addParquetTablesSections(tables,subsection); 
							$rootScope.$broadcast(EVENTS.TABLES_UPDATED,tables);
							subsection.loading = false;
						});
				};		
			});


		querySections.push(parquetSection);
		sections.push(runQuerySection);
		
		fsSections = [];
		fsSections.push({
			name: 'Tachyon',
			type: 'link',
			url: '/browse/tachyon',
			title: 'Browse Tachyon'
		});		
		fsSections.push({
			name: 'HDFS',
			type: 'link',
			url: '/browse/hdfs',
			title: 'Browse HDFS'
		});
		sections.push({
			name: 'MAP FILES',
			type: 'toggle',
			url: '',
			pages: fsSections
		});

		$rootScope.$on('$locationChangeSuccess', onLocationChange);
				
		/**
		* look through all the menu sections to find the page that should be selected based on URL
		*/
		function findSelectedPage(sections,matchPage) {
			sections.forEach(function(section) {
				if (matchPage(section, section)) {
				}
				else if (section.pages) {
					// matches section toggles
					findSelectedPage(section.pages,matchPage);
				} 
			});		
		}

		/**
		* actions to take when URL changes
		*/
		function onLocationChange() {
			var path = $location.path();
			var matchPage = function(section, page) {
				
				if (path === page.url) {
					self.selectSection(section);
					self.selectPage(section, page);
					return true;
				}
				return false;
			};
			
			findSelectedPage(sections,matchPage);

		}
		
		var self;

		self = {
			sections: sections,

			selectSection: function(section) {
				self.openedSection = section;
			},
			
			toggleSelectSection: function(section) {
				var wasOpen = self.openedSection === section;
				self.openedSection = (wasOpen ? section.parent : section);
				if (!wasOpen && self.openedSection.type === 'toggle' && self.openedSection.populateSubsections) {
					
					self.openedSection.populateSubsections();
			
				}
			},
			
			isSectionSelected: function(section) {
				return self.openedSection === section;
			},

			selectPage: function(section, page) {
				if  (page && page.url)
					$location.path(page.url);
				self.currentSection = section;
				self.currentPage = page;
				

			},
			isPageSelected: function(page) {
				return self.currentPage === page;
			}
		};
		
		return self;		
	}
]);
