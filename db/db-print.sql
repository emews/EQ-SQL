
/*
 DB PRINT SQL
 Just dump the tables for human inspection
 See db-print.sh for usage
*/

\dt

\echo == EMEWS GROUPS ==
select * from emews_groups;
\echo == EMEWS POINTS ==
select * from emews_points;

\echo == EMEWS QUEUE IN ==
select * from emews_queue_IN;
\echo == EMEWS QUEUE OUT ==
select * from emews_queue_OUT;
