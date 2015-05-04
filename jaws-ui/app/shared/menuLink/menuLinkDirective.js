angular.module('JawsUI.menuLinkDirective', [])
.directive('menuLink', function() {
	return {
		scope: {
			section: '='
		},
		templateUrl: 'shared/menuLink/menu-link.html',
		link: function($scope, $element) {
			var controller = $element.parent().controller();

			$scope.isSelected = function() {
				return controller.isSelected($scope.section);
			};
			
			$scope.getSectionURL = function() {
				return ($scope.section.url && $scope.section.url!=="")? "#"+$scope.section.url:"";
			};

			$scope.focusSection = function() {
				// set flag to be used later when
				// $locationChangeSuccess calls openPage()
				controller.autoFocusContent = true;
			};
		}
	};
});