angular.module('JawsUI.promptDialog.controller', [])
.controller('PromptDialog', [
	'$scope',
	'$mdDialog',
	'prompt',
	'defaultValue',
	function($scope, $mdDialog, prompt, defaultValue) {
		$scope.prompt = prompt;
		$scope.value = defaultValue;

		$scope.cancel = function() {
			$mdDialog.cancel();
		};
		$scope.answer = function() {
			$mdDialog.hide($scope.value);
		};
	}
]);