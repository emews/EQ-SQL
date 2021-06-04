
# DB CREATE PY
# Initialize the SQLite DB for COVID workflows
# See db-create.sql for the table schema

import os, sys


def create_tables(DB, workflow_sql):
    """ Set up the tables defined in the SQL file """
    print("creating tables: " + workflow_sql)
    DB.connect()
    with open(workflow_sql) as fp:
        sqlcode = fp.read()
    DB.executescript(sqlcode)
    DB.commit()


def main(db_file):

    if os.path.exists(db_file):
        print("DB file already exists: " + db_file)
        exit(1)
    
    # Catch and print all exceptions to improve visibility of
    #       success and failure
    success = False
    try:
        from db_tools import workflow_sql
        print("workflow_sql ...")
        DB = workflow_sql(db_file, log=True)
        print("DB OK")
        this = os.getenv("EMEWS_PROJECT_ROOT")
        workflow_sql = this + "/db/workflow.sql"
        create_tables(DB, workflow_sql)
        success = True
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        with open("python-errors.txt", "a") as fp:
            fp.write(traceback.format_exc())
            fp.write("\n")

    if not success:
        print("DB: !!! INIT FAILED !!!")
        exit(1)

    print("DB: initialized successfully")

if __name__ == "__main__" and 'db_file' not in globals():
    import argparse
    parser = argparse.ArgumentParser(description="Setup the workflow DB.")
    parser.add_argument("db", action="store", help="specify DB file")
    parser.add_argument("id", action="store", help="specify new DB ID")
    args = parser.parse_args()
    argvars = vars(args)

    db_file = argvars["db"]
    main(db_file)
