
# DB SETTINGS SH
# Source this for DB settings
# Site-specific settings are in env-*.sh
# Provide -d to require the DB_DATA shell variable
# Provide -n to require the DB_NAME shell variable
# Provide -v for verbose output

REQUIRE_DB_DATA=0
REQUIRE_DB_NAME=0
VERBOSE=0

ERROR=0
# Critical for use under source:
OPTIND=1
while getopts "dnv" OPT
do
  case $OPT in
    d) REQUIRE_DB_DATA=1 ;;
    n) REQUIRE_DB_NAME=1 ;;
    v) VERBOSE=1         ;;
    ?) ERROR=1 ; break   ;; # Bash prints an error
  esac
done

if (( ERROR ))
then
  echo "db-settings.sh: argument error!"
  return 1
fi

if (( REQUIRE_DB_DATA ))
then
  if [[ ${DB_DATA:-} == "" ]]
  then
    echo "Set DB_DATA!"
    return 1
  fi
fi

# Set default DB settings
# The host serving the DB
export DB_HOST=${DB_HOST:-localhost} # thetalogin4
# The port serving the DB
export DB_PORT=${DB_PORT:-11219}
# The user name to use for the DB
export DB_USER=${DB_USER:-}
export DB_NAME=${DB_NAME:-EQ_SQL}
export DB_MODE=${DB_MODE:-SOFT} # Choices: ON, SOFT, OFF
export DB_DATA

PATH=/projects/Swift-T/public/sfw/theta/postgres-12.2/bin:$PATH

db-settings()
# Can use this in an interactive shell too
{
  echo DB_HOST=$DB_HOST
  echo DB_PORT=$DB_PORT
  echo DB_NAME=$DB_NAME
  echo DB_MODE=$DB_MODE
  echo DB_USER=${DB_USER:-default:${USER}}
  # The data directory for the DB
  echo DB_DATA=${DB_DATA:-unset}
}

sql()
# Can use this in an interactive shell too
{
  local USER_ARG=""
  if (( ${#DB_USER} ))
  then
    USER_ARG=( --user $DB_USER )
  fi
  psql --host=$DB_HOST --port=$DB_PORT $USER_ARG $DB_NAME $*
}

log_hosts()
# Log message regarding SQL server host operations
{
  local U # USER
  printf -v U "%-8s" $USER
  echo $(date "+%Y-%m-%d %H:%M:%S") "$U" "$*" | tee -a $DB_DATA/hosts.log
}

if (( VERBOSE ))
then
  echo "DB SETTINGS:"
  db-settings
  echo
fi
