
/**
    WORKFLOW SQL
    Initialize the PSQL DB for workflows
    See db-create.py for usage
*/

/* For SQLite: */
/* PRAGMA foreign_keys = ON; */

/* Each group here is a collection of other groups or points
*/
create table emews_groups(
       /* auto-generated integer */
       ID serial primary key,
       /* e.g. 'experiment':'X-032' or 'iteration':421 */
       json_label text,
       /* the parent group ID of this point.  May point nowhere. */
       group_ integer,
       /* creation time */
       time timestamp
);

/* Each row here is a model run
*/
create table emews_points(
       ID integer,
       /* the group ID of this point */
       group_ integer,
       /* See db_covid.py for valid status codes */
       status integer,
       /* JSON-formatted input values */
       json_in  text,
       /* JSON-formatted output values */
       json_out text,
       /* time this point was created (json_in) */
       time_start timestamp,
       /* time this point was finished (json_out) */
       time_stop  timestamp,
       foreign key (group_) references emews_groups(ID)
);

/* This generator is just for the queues */
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
