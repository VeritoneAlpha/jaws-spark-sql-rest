/**
* based on lrInfiniteScroll: https://github.com/lorenzofox3/lrInfiniteScroll
* by lorenzofox3 <laurent34azerty@gmail.com>
*
* modified by Liviu Iancuta <liviu.iancuta@gmail.com> to check if scrollbar 
* is not present and call handler until it is
*/

(function (ng) {
    'use strict';
    var module = ng.module('lrInfiniteScroll', []);

    module.directive('lrInfiniteScroll', [
		'$timeout', 
		function ($timeout) {
			return {
				link: function (scope, element, attr) {
					var
						lengthThreshold = attr.scrollThreshold || 50,
						timeThreshold = attr.timeThreshold || 400,
						scrollBusy = attr.scrollBusy || false,
						handler = scope.$eval(attr.lrInfiniteScroll),
						handlerPromise = null,
						scrollTimeoutPromise = null,
						lastRemaining = 9999;

					lengthThreshold = parseInt(lengthThreshold, 10);
					timeThreshold = parseInt(timeThreshold, 10);
					
					scope.$on('$destroy', function (ev) {
						$timeout.cancel(handlerPromise);
						$timeout.cancel(scrollTimeoutPromise);
					});

					if (!handler || !ng.isFunction(handler)) {
						handler = ng.noop;
					}
					
					function checkIfScrollPresent() {
						if (element[0].scrollHeight>0 && element[0].scrollHeight <= element[0].clientHeight) {
							if (scope.$eval(scrollBusy)) {	
								scrollTimeoutPromise = $timeout(function () { 
										checkIfScrollPresent(); 
								}, timeThreshold);					
							}
							else
								fireHandler();
						}
					}
					
					function fireHandler() {
						//if there is already a timer running which has no expired yet we have to cancel it and restart the timer
						$timeout.cancel(scrollTimeoutPromise);
						$timeout.cancel(handlerPromise);
						
						handlerPromise = $timeout(function () {
							handler();
							handlerPromise = null;
							checkIfScrollPresent();
						}, timeThreshold);
					}
					

							
					element.bind('scroll', function () {
						var
							remaining = element[0].scrollHeight - (element[0].clientHeight + element[0].scrollTop);

						//if we have reached the threshold and we scroll down
						if (remaining < lengthThreshold && (remaining - lastRemaining) < 0) {
							fireHandler();
						}
						lastRemaining = remaining;
					});
					
					scrollTimeoutPromise = $timeout(function () { 
						checkIfScrollPresent();
					}, 100);
				}
			};
		}]);
})(angular);
