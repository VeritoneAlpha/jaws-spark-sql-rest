angular.module('JawsUI.filters', [])

.filter('nospace', function() {
	return function(value) {
		return (!value) ? '' : value.replace(/ /g, '');
	};
});