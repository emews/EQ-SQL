import logging
import os
import subprocess
import socket
from typing import Union
from importlib import resources
from typing import List
import psycopg2


def setup_log(log_name, log_level, procname=""):
    logger = logging.getLogger(log_name)
    handlr = logging.StreamHandler()
    formtr = logging.Formatter("%(asctime)s " + procname
                               + " %(name)-9s %(message)s",
                               datefmt="%Y-%m-%d %H:%M:%S")
    handlr.setFormatter(formtr)
    logger.addHandler(handlr)
    logger.setLevel(log_level)
    return logger


class WorkflowSQL:

    def __init__(self, host="127.0.0.1", port=5432,
                 user=os.environ['USER'],
                 dbname="EQ_SQL",
                 envs=False,
                 log_level=logging.WARN,
                 procname=""):
        """
        Sets up a wrapper around the SQL connection and cursor objects
        Also caches dicts that convert between names and ids for the
        features and studies tables
        envs: If True, self-configure based on the environment
        """
        self.conn = None
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        if envs:
            self.configure_envs()
        self.autoclose = True
        self.procname = procname  # a unique process name
        self.logger = setup_log(__name__, log_level, self.procname)
        self.info("Initialized.")

    def configure_envs(self):
        def env_has(k):
            v = os.getenv(k)
            if v is None:
                return False
            if len(v.strip()) == 0:
                return False
            return True

        if env_has("DB_HOST"):
            self.host = os.getenv("DB_HOST")
        if env_has('DB_USER'):
            self.user = os.getenv('DB_USER')
        if env_has("DB_PORT"):
            try:
                port_string = os.getenv("DB_PORT")
                self.port = int(port_string)
            except ValueError as e:
                self.logger.fatal("DB_PORT is not an integer: "
                                  + "got: '%s'" % port_string)
                raise e

        if env_has("DB_NAME"):
            self.dbname = os.getenv("DB_NAME")

    def connect(self):
        import psycopg2
        if self.conn is None:
            self.info(f"connect(): connecting to {self.host} {self.port} as {self.user}")
            try:
                if self.port is None:
                    self.conn = psycopg2.connect(f"dbname={self.dbname}",
                                                 host=self.host,
                                                 user=self.user)
                else:
                    self.conn = psycopg2.connect(f"dbname={self.dbname}",
                                                 host=self.host,
                                                 port=self.port,
                                                 user=self.user)
            except psycopg2.OperationalError as e:
                self.info("connect(): could not connect!")
                raise ConnectionException(e)
            self.info("connect(): connected.")
            self.debug(f"connect(): {self.conn}")

        else:
            if self.conn != "DISABLED":
                self.info("connect(): Already connected.")
        return "OK"

    def close(self):
        self.autoclose = False
        self.conn.close()
        self.conn = None

    def debug(self, message):
        if self.logger:
            self.logger.debug(message)

    def info(self, message):
        if self.logger:
            self.logger.info(message)

    def fatal(self, message):
        if self.logger:
            self.logger.fatal(message)
        else:
            print(message)

    def __del__(self):
        if not self.autoclose:
            return
        try:
            self.conn.commit()
            self.conn.close()
        except:  # noqa E722
            pass
        self.info("DB auto-closed.")


# def format_insert(table, names, values):
#     if len(names) != len(values):
#         raise ValueError("lengths of names, values must agree!")
#     names_tpl  = sql_tuple(names)
#     values_tpl = sql_tuple(values)
#     cmd = f'insert into {table} {names_tpl} values {values_tpl};'

#     return cmd

def format_insert(table, names):
    names_phrase = f'({",".join(names)})'
    placeholders = ', '.join(['%s'] * len(names))
    cmd = f'insert into {table} {names_phrase} values ({placeholders})'
    return cmd


def format_update(table, names, where):
    assign_list = [f'{name} = %s' for name in names]
    assigns = ', '.join(assign_list)
    cmd = f'update {table} set {assigns} where {where}'
    return cmd


def format_select(table, what, where=None):
    ''' Do a SQL select '''
    where_clause = ''
    if where is not None:
        where_clause = f' where {where}'
    cmd = f'select {what} from {table} {where_clause}'

    return cmd


def Q(s):
    """ Quote the given string """
    return "'" + str(s) + "'"


def QL(L):
    """ Quote-List: Quote each list entry as a string """
    return map(Q, L)


def QA(*args):
    """ Quote-Arguments: Quote each argument as a string,
        return list
    """
    return list(map(Q, args))


def sql_tuple(L):
    """ Make the given list into a SQL-formatted tuple """
    L = list(map(str, L))
    result = ""
    result += "("
    result += ",".join(L)
    result += ")"
    return result


def sql_tuple_q(L):
    """ Make the given list into a Quoted SQL-formatted tuple """
    L = list(map(str, L))
    result = ""
    result += "("
    result += ",".join(QL(L))
    result += ")"
    return result


class ConnectionException(Exception):
    def __init__(self, cause):
        """ cause: another Exception """
        self.cause = cause


def _run_cmd(cmd: List, start_msg, failure_msg, end_msg=None, print_result=True):
    try:
        print(start_msg)
        result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, check=True)
        result_str = result.stdout.decode('utf-8')
        if end_msg is not None:
            print(end_msg)
        if print_result:
            print(result_str)
    except subprocess.CalledProcessError as ex:
        if ex.stdout is None:
            print(failure_msg)
        else:
            print(failure_msg + ex.stdout.decode('utf-8'))
        raise ValueError()


def _exec_sql(sql_file: Union[str, bytes, os.PathLike], db_user: str = 'eqsql_user',
              db_name: str = 'EQ_SQL', db_host: str = 'localhost', db_port: int = None):
    """Executes the SQL commands in the specified sql_file.

    Args:
        sql_file: a file containing the SQL to execute
        db_user: the database user name
        db_name: the name of the database
        db_host: the hostname where the database server is located
        db_port: the port of the database server.
    """
    conn = psycopg2.connect(f'dbname={db_name}', user=db_user, host=db_host, port=db_port)
    with conn:
        with conn.cursor() as cur:
            with open(sql_file, 'r') as sql:
                cur.execute(sql.read())

            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            print("Tables:")
            for row in cur.fetchall():
                print(f'\t{row[0]}')

    conn.close()


def init_eqsql_db(db_path: str, create_db_sql_file: Union[str, bytes, os.PathLike] = None,
                  db_user='eqsql_user', db_name='EQ_SQL', db_port=None):
    """Creates and initialized an EQSQL postgresql database.

    This will:
        1. Create a database "cluster" at the specified path.
        2. Start the server instance using that cluster.
        3. Create the specified user.
        4. Create the specified database in that cluster.
        5. Populate that database with tables etc. by executing the commands in the
           specified file.
        6. Stop the database server.

    Args:
        db_path: the file path for the database cluster. This must not exist.
        create_db_sql_file: a file containing the SQL to execute to create the database tables etc.
            If this is None (the default) the default EQSQL SQL schema will be used.
        db_user: the database user name
        db_name: the name of the database
        db_host: the hostname where the database server is located
        db_port: the port of the database server.
    """
    try:
        _run_cmd(['which', 'initdb'], 'Checking for initdb ...',
                 'EQ/SQL create database failed: "initdb" command not found',
                 print_result=True)
        _run_cmd(['initdb', '-D', db_path], f'Initializing database directory: {db_path} ...',
                 'EQ/SQL create database failed:', 'Database directory initialized',
                 False)
        if db_port is None:
            cmd = ['pg_ctl', '-D', db_path, '-l', f'{db_path}/db.log', '-o', '-F', 'start']
        else:
            cmd = ['pg_ctl', '-D', db_path, '-l', f'{db_path}/db.log', f'-o -F -p {db_port}', 'start']
        _run_cmd(cmd, f'Starting database with log: {db_path}/db.log',
                 'EQ/SQL create database failed: error starting database server', 'Database server started', False)

        port_arg = []
        args = ['createuser', '-w', db_user]
        if db_port is not None:
            port_arg = ['-p', f'{db_port}']
        _run_cmd(args + port_arg,
                 f'\nCreating database user {db_user}',
                 'EQ/SQL create database failed: error creating user', 'User created', False)

        args = ['createdb', f'--owner={db_user}', db_name]
        _run_cmd(args + port_arg,
                 f'\nCreating {db_name} database',
                 'EQ/SQL create database failed: error creating user', 'Database created', True)

        print("\nCreating EQ/SQL database tables")
        if create_db_sql_file is None:
            create_db_sql_file = resources.files('eqsql').joinpath('workflow.sql')
        _exec_sql(create_db_sql_file, db_name=db_name, db_user=db_user, db_port=db_port)

        return (db_path, db_user, db_name, socket.getfqdn(), db_port)

    except ValueError:
        pass

    finally:
        try:
            _run_cmd(['pg_ctl', '-D', db_path, 'stop'], '\nStopping database server', '', '', True)
        except ValueError:
            pass


def start_db(db_path: Union[str, bytes, os.PathLike], pg_ctl: Union[str, bytes, os.PathLike] = 'pg_ctl',
             db_port: int = None):
    """Starts the postgresql database cluster on the specified path

    Args:
        db_path: the file path for the database cluster to start
        pg_ctl: path to postgresql's pg_ctl executable
        db_port: the port number to start the db on
    """
    try:
        if db_port is None:
            cmd = [pg_ctl, '-D', db_path, '-l', f'{db_path}/db.log', '-o', '-F', 'start']
        else:
            cmd = [pg_ctl, '-D', db_path, '-l', f'{db_path}/db.log', f'-o -F -p {db_port}', 'start']
        _run_cmd(cmd,
                 f'\nStarting database with log:{db_path}/db.log',
                 'EQ/SQL create database failed: error starting database server', 'Database server started', False)
    except ValueError:
        pass


def stop_db(db_path: Union[str, bytes, os.PathLike], pg_ctl: Union[str, bytes, os.PathLike] = 'pg_ctl',
            db_port: int = None):
    """Stops the postgresql database cluster on the specified path

    Args:
        db_path: the file path for the database cluster to stop
        pg_ctl: path to postgresql's pg_ctl executable
        db_port: the port number of the database to stop
    """
    try:
        if db_port is None:
            cmd = [pg_ctl, '-D', db_path, 'stop']
        else:
            cmd = [pg_ctl, '-D', db_path, f'-o -F -p {db_port}', 'stop']
        _run_cmd(cmd, '\nStopping database server', '', '', True)

    except ValueError:
        pass


def reset_db(db_user: str = 'eqsql_user', db_name: str = 'EQ_SQL', db_host: str = 'localhost',
             db_port: int = None):
    """Resets the database by deleting the contents of all the eqsql tables and restarting
    the emews task id generator sequence.

    Args:
        db_user: the database user name
        db_name: the name of the database
        db_host: the hostname where the database server is located
        db_port: the port of the database server.
    """
    clear_db_sql = """
        delete from eq_exp_id_tasks;
        delete from eq_tasks;
        delete from emews_queue_OUT;
        delete from emews_queue_IN;
        delete from eq_task_tags;
        alter sequence emews_id_generator restart;
    """

    conn = psycopg2.connect(f'dbname={db_name}', user=db_user, host=db_host, port=db_port)
    with conn:
        with conn.cursor() as cur:
            cur.execute(clear_db_sql)

    conn.close()
