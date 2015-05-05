angular.module('JawsUI.menuToggleDirective', [])
.directive('recursive', [
	'$compile',
	function($compile) {
    return {
        restrict: "EACM",
        priority: 100000,
        compile: function(tElement, tAttr) {
            var contents = tElement.contents().remove();
            var compiledContents;
            return function(scope, iElement, iAttr) {
                if(!compiledContents) {
                    compiledContents = $compile(contents);
                }
                iElement.append(
                    compiledContents(scope, 
                                     function(clone) {
                                         return clone; }));
            };
        }
    };
}])
.directive('menuToggle', function(){
	return {
		scope: {
			section: '='
		},
	
		templateUrl: 'shared/menuToggle/menu-toggle.html',
		link: function($scope, $element) {
			var controller = $element.parent().controller();

			
			$scope.removeSection = function(section) {
				return controller.removeSection(section);
			};
			$scope.isOpen = function() {
				return controller.isOpen($scope.section);
			};
			$scope.toggle = function() {
				controller.toggleOpen($scope.section);
			};
			
			$scope.isSelected = function() {
				return controller.isSelected($scope.section);
			};
			
			$scope.getSectionURL = function() {
				return ($scope.section.url && $scope.section.url!=="")? "#"+$scope.section.url:"";
			};			

			var parentNode = $element[0].parentNode.parentNode.parentNode;
			if (parentNode.classList.contains('parent-list-item')) {
				var heading = parentNode.querySelector('h2');
				$element[0].firstChild.setAttribute('aria-describedby', heading.id);
			}
		}
	};
});