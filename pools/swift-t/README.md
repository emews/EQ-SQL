# Swift Worker Pool

To run the worker pool on Bebop, assuming db has been previously
initialized (i.e., the appropriate tables are already there):

```
$ source EQ-SQL/db/env-bebop.sh
$ EQ-SQL/db/db-start.sh
$ source /lcrc/project/EMEWS/bebop/envs/bebop_swift_9ad37bb.sh
# Edit DB_HOST etc. in data/cfgs/bebop.cfg as necessary
$ cd scripts
$ ./bebop_start_loop.sh <exp_id> ../data/cfgs/bebop.cfg
```

To run the test where the deap algorithm.py ME uses the
bebop worker pool, do the above, and then from a different
terminal or machine (via ssh tunnel)

```
$ cd test
# Edit DB_HOST, LD_LIBRARY_PATH, etc. in run_deap_me.sh as necessary.
$ ./run_deap_me.sh
```

On Bebop, you'll need to load an appropriate python (e.g., `module load anaconda3/2020.11`).
