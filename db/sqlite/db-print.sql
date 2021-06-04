
/*
 DB PRINT SQL
 Just dump the tables for human inspection
 See db-print.sh for usage
*/

.headers on
.mode columns

.tables

select * from run_ids;
select * from run_steps;
-- select subplan_id, status from runhist;
