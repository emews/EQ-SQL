# EMEWS QUEUES in SQL for Python #

The eqsql package provies an API for HPC workflows to submit tasks (such as
simulation model runs) to a queue implemented in a database. eqsql worker pools pop tasks 
off this queue for evaluation, and push the results back to a database input queue. 
The tasks can be provided by a Python or R language model exploration (ME) algorithm.

A task is submitted with the following arguments: an experiment id; the task work type; the task payload; an optional
priority that defaults to 0; and an optional metadata tag string. The payload contains sufficient information for a
worker pool to execute the task and is typically a JSON formatted string, either a JSON dictionary or in less complex
cases a simple JSON list. On submission, the API creates a unique task identifier (an integer) for the task and 
inserts that identifier, the experiment identifier, the work type, and the payload into the EMEWS DB tasks table,
together with a task creation timestamp. That task identifier, priority and work type are then inserted into the EMEWS
DB output queue table.

API docs are [here](https://emews.github.io/eqsql/apidoc/)
