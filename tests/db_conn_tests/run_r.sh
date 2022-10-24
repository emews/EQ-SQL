WORKING_DIR=$TURBINE_OUTPUT

cd $WORKING_DIR
rfile=$1

# For a python model:
# "$EMEWS_PROJECT_ROOT/python/run_model.py"

arg_array=( "$EMEWS_PROJECT_ROOT/db_conn_tests/${rfile}" )

# For an R model:
echo "CRCSPIN.SH: USING Rscript:"
which Rscript
echo

Rscript "${arg_array[@]}"