var app = angular.module('JawsUI', [
		'ngMaterial',
		'ngRoute',
		'JawsUI.menuLinkDirective',
		'JawsUI.menuToggleDirective',
		'JawsUI.menuService',
		'JawsUI.filters', 
		'JawsUI.constants',
		'JawsUI.config',
		'JawsUI.query.controller',
		'JawsUI.fileBrowse.controller',
		'JawsUI.jawsService',
		'angular-growl'
		])
		
.config([
	'$routeProvider',
	'$mdThemingProvider',
	'CONFIG',
	function($routeProvider, $mdThemingProvider,CONFIG) {
		$routeProvider.
		when ('/', {
			redirectTo: 'query',
		}).
		when('/browse/:fileSystem', {
			templateUrl: 'components/fileBrowse/browse-files.html',
		}).		
		otherwise({
			templateUrl: 'components/query/query.html',
		});
		
		//fix background for bug in angular material 0.8.3 
		var background = $mdThemingProvider.extendPalette('grey', {
			'A100': '#f8f8f8'
		});
		$mdThemingProvider.definePalette('background', background);		

		$mdThemingProvider.theme('default')
			.primaryPalette(CONFIG.COLOR_PALLETE)
			.backgroundPalette('background');
	}
])

.controller('AppCtrl', [
	'$rootScope',
	'$scope',
	'$mdSidenav',
	'$timeout',
	'$mdDialog',
	'$log',
	'menu',
	'EVENTS',
	'growl',
	'jawsService',
	function($rootScope,$scope, $mdSidenav, $timeout, $mdDialog, $log, menu, EVENTS,growl,jawsService) {
		var self = this;
		
		// Methods used by menuLink and menuToggle directives
		this.openPage = openPage;
		this.isOpen = isOpen;
		this.isSelected = isSelected;
		this.toggleOpen = toggleOpen;
		this.removeSection = removeSection;
		this.autoFocusContent = false;
		var mainContentArea = document.querySelector("[role='main']");
	
		$scope.menu = menu;
		$scope.currentPageTitle = "";

		this.closeMenu = function(menuId) {
			$timeout(function() {
				$mdSidenav(menuId).close();
			});

		};

		$scope.toggleSidenav = function(menuId) {
			$timeout(function() {
				$mdSidenav(menuId).open();
			});

		};
		
		$rootScope.$on('$locationChangeSuccess', openPage);

		$scope.showError = function(message) {
			$log.error(message);
			growl.error(message, {ttl: 5000});		
		};
		
		$scope.showConfirmation = function(message) {
			$log.log(message);
			growl.success(message, {ttl: 5000});			
		};		
		
		$scope.showInfo = function(message) {
			$log.info(message);
			growl.info(message, {ttl: 5000});		
		};		
		
		$scope.warnNetworkError = function() {
			$scope.showError('A network error occured');
		};


		/**
		* Remove a section from menu
		* @param {Object} section the menu section to remove
		*/
		function removeSection(section) {
			if (section.contentType && section.contentType === 'ptable') {
				//section is a parquet table so remove table
				var confirm = $mdDialog.confirm()
					.content('Are you sure you want to delete the '+section.name+' parquet table?')
					.ok('Yes')
					.cancel('No');
				$mdDialog.show(confirm).then(function() {  
					jawsService.deleteParquetTable(section.name)
						.then(function(response) {
							$scope.showConfirmation("Table deleted!");
							//remove from menu
							if (section.parent) {
								var idx = section.parent.pages.indexOf(section);
								if (idx>=0)
									section.parent.pages.splice(idx,1);
							}
						},function(response) {$scope.warnNetworkError();});
					
				});
			}
			
		}
		
		function openPage() {
			if (self.autoFocusContent) {
				self.closeMenu('left');
				focusMainContent();
				self.autoFocusContent = false;
			}
			if(menu.currentSection)
				$scope.currentPageTitle=menu.currentSection.title;
		}
		
		function focusMainContent($event) {
			// prevent skip link from redirecting
			if ($event) {
				$event.preventDefault();
			}

			$timeout(function() {
				mainContentArea.focus();
			}, 90);
		}

		function isSelected(page) {
			return menu.isPageSelected(page);
		}

		function isSectionSelected(section) {
			var selected = false;
			var openedSection = menu.openedSection;

			if (openedSection === section) {
				selected = true;
			} else if (section.pages) {

				section.pages.forEach(function(childSection) {
					if (isSectionSelected(childSection)) {
						selected = true;
					}
				});
			}
			return selected;
		}

		function isOpen(section) {

			return isSectionSelected(section);
		}

		function toggleOpen(section) {
			menu.toggleSelectSection(section);
		}
	}
]);