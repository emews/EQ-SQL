# EQSQL Python Docs README #

## API Docs ##

The API docs are built with sphinx which can be installed
with pip or conda.

API doc source in source/.

To build the docs:

```bash
$ make clean
$ make html
```

or alternatively,

```bash
$ ./build.sh
```

which will do the above and copy the html output into the
website repository.
