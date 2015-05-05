/**
 * AngularJS fixed header scrollable table directive
 * @author Jason Watmore <jason@pointblankdevelopment.com.au> (http://jasonwatmore.com)
 * @version 1.2.0
 *
 * modified by Liviu Iancuta <liviu.iancuta@gmail.com> to properly
 * display table headers if horizontal scrollbar is present
 * or if new rows are added after initial load
 */
(function () {
    angular
        .module('anguFixedHeaderTable', [])
        .directive('fixedHeader', fixedHeader);

    fixedHeader.$inject = ['$timeout'];

    function fixedHeader($timeout,element) {
        return {
            restrict: 'A',
            link: link
        };

        function link($scope, $elem, $attrs, $ctrl) {
            var elem = $elem[0];

            // wait for data to load and then transform the table
            $scope.$watch(tableDataLoaded, function(isTableDataLoaded) {
                if (isTableDataLoaded) {
                    transformTable();
                }
            });

            function tableDataLoaded() {
                // last cell in the tbody exists when data is loaded but doesn't have a width
                // until after the table is transformed. We use last row so that we can transform
				// the table again if new rows are added after initial transform
                var firstCell = elem.querySelector('tbody tr:last-child td:first-child');
                return firstCell && !firstCell.style.width;
            }

            function transformTable() {
				tHead=angular.element(elem.querySelectorAll('thead'));
				tBody=angular.element(elem.querySelectorAll('tbody'));
				
				//we want to monitor scrolling in table body so we can scroll table header so columns match
				tBody.bind("scroll", function() {
					tHead.scrollLeft(tBody.scrollLeft());
				});		
							
                // reset display styles so column widths are correct when measured below
                angular.element(elem.querySelectorAll('thead, tbody, tfoot')).css('display', '');

                // wrap in $timeout to give table a chance to finish rendering
                $timeout(function () {
                    // set widths of columns
					var columnHeight;
                    angular.forEach(elem.querySelectorAll('tr:first-child th'), function (thElem, i) {

                        var tdElems = elem.querySelector('tbody tr:first-child td:nth-child(' + (i + 1) + ')');
                        var tfElems = elem.querySelector('tfoot tr:first-child td:nth-child(' + (i + 1) + ')');

                        var columnWidth = tdElems ? Math.max(tdElems.offsetWidth,thElem.offsetWidth) : thElem.offsetWidth;
                        columnHeight = thElem.offsetHeight;
                        if (tdElems) {
                            tdElems.style.width = columnWidth + 'px';
                            tdElems.style.minWidth = columnWidth + 'px';
                        }
                        if (thElem) {
                            thElem.style.width = columnWidth + 'px';
                            thElem.style.minWidth = columnWidth + 'px';
                            thElem.style.maxWidth = columnWidth + 'px';
                        }
                        if (tfElems) {
                            tfElems.style.width = columnWidth + 'px';
                        }
                    });

                    // set css styles on thead and tbody
                    angular.element(elem.querySelectorAll('thead, tfoot')).css({
						'display': 'block',
						'overflow': 'hidden',
						'position': 'absolute',
						'left': '0px',
						'top': '0px',
						'right': '0px'
						});

                    angular.element(elem.querySelectorAll('tbody')).css({
                        'display': 'block',
						'overflow': 'auto',
						'position': 'absolute',
						'left': '0px',
						'top': columnHeight+'px',
						'right': '0px',
						'bottom': '0px'
                    });

                    // reduce width of last column by width of scrollbar
                    var tbody = elem.querySelector('tbody');
                    var scrollBarWidth = tbody.offsetWidth - tbody.clientWidth;
                    if (scrollBarWidth > 0) {
						//enlarge horizontal scrolling when reaching if vertical scroll is present
                        var lastColumn = elem.querySelector('thead tr:first-child th:last-child');
                        lastColumn.style.width = (lastColumn.offsetWidth + scrollBarWidth) + 'px';
                        lastColumn.style.minWidth = lastColumn.style.width;
                        lastColumn.style.maxWidth = lastColumn.style.width;
					
                        // for some reason trimming the width by 2px lines everything up better
                        scrollBarWidth -= 2;
                        lastColumn = elem.querySelector('tbody tr:first-child td:last-child');
                        lastColumn.style.width = (lastColumn.offsetWidth - scrollBarWidth) + 'px';

                    }
                });
            }
        }
    }
})();