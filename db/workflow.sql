
/**
    WORKFLOW SQL
    Initialize the PSQL DB for workflows
    See db-create.py for usage
*/

/* For SQLite: */
/* PRAGMA foreign_keys = ON; */

/* The main table, one row for each run
   The workflow ID (expid) is specified by the user and is conventionally
   the same as the EMEWS experiment ID.
   The exp_int is a SQL integer that is the main key for the tables.
*/
create table expids(
       /* auto-generated integer */
       exp_int serial primary key,
       /* string name e.g. 'X-032' */
       expid character varying(32) unique,
       /* JSON-formatted input values */
       json_in  text,
       /* creation time */
       time timestamp
);

/* Each experiment has multiple model instances defined here
   "instance" is a SQL reserved word- we use "instnce"
*/
create table exp_instnces(
       exp_int integer,
       step integer,
       /* the ID of this instance */
       instnce integer,
       /* See db_covid.py for valid status codes */
       status integer,
       /* JSON-formatted input values */
       json_in  text,
       /* JSON-formatted output values */
       json_out text,
       time_start timestamp,
       time_stop  timestamp,
       foreign key (exp_int) references expids(exp_int)
);

-- /* Each instance has a params dict, divided into key/value keyname/content pairs here
--    "key"/"value" are SQL reserved words- we use "keyname"/"content"
-- */
-- create table exp_instnce_params(
--        exp_int integer,
--        step    integer,
--        instnce integer,
--        keyname text,
--        content text,
--        foreign key (exp_int) references expids(exp_int)
-- );

/* Each model run has one entry here */
create table exp_runs(
       exp_int integer,
       step    integer,
       instnce integer,
       run     integer,
       status  integer,
       /* JSON-formatted input values */
       json_in  text,
       /* JSON-formatted output values */
       json_out text,
       time_start timestamp,
       time_stop  timestamp,
       foreign key (exp_int) references expids(exp_int)
);

create sequence emews_id_generator start 1 no cycle;

create table emews_queue_OUT(
       eq_id integer,
       json  text,
       claimed integer
);

create table emews_queue_IN(
       eq_id integer,
       json  text
);
