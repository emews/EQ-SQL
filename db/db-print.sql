
/*
 DB PRINT SQL
 Just dump the tables for human inspection
 See db-print.sh for usage
*/

\dt

\echo == EXPIDS ==
select * from expids;
\echo == INSTANCES ==
select * from exp_instnces;
\echo == RUNS ==
select * from exp_runs;
