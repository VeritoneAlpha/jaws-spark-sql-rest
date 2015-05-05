angular.module('JawsUI.constants', [])
.constant('EVENTS', {
	TABLES_UPDATED:'tables-updated',
	COLUMNS_UPDATED:'columns-updated',
	DB_SWITCHED:'database-switched'
})
.constant('QUERY_STATUS', {
	DONE:'DONE',
	IN_PROGRESS:'IN_PROGRESS',
	FAILED:'FAILED'
});
