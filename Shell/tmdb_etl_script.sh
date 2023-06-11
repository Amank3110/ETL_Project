#!/bin/bash
. /mnt/c/Users/Aman/Documents/myProject/Properties/myProject_Directory.properties
. /mnt/c/Users/Aman/Documents/myProject/Properties/myProject_Access.properties

LOGFILE="tmdb_etl_script.sh_$(date +%Y%m%d%H%M%S).log"
LOG_PATH="$LOG_DIR/$LOGFILE"

log_error() {
    local step_number=$1
    local error_message=$2
    local timestamp=$(date +%H:%M:%S)
    echo "ERROR Step $step_number $timestamp: $error_message" >> $LOG_PATH
}

STEP_NUMBER='1'
    bash $SHELL_DIR/tmdb_script_sample.sh

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to load data into RAW_TMDB_DATA table"
        exit 1
    fi

STEP_NUMBER='2'
    SF_QUERY_TAG="truncate_from_inter_tmdb_data_${STEP_NUMBER}"
    echo "Truncate INTER_TMDB_DATA table $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c $PROJECT_CONNECTION_NAME -q "TRUNCATE TABLE myDb.mySchema.INTER_TMDB_DATA" -o exit_on_error=true -D "SF_QUERY_TAG=${SF_QUERY_TAG}"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to Truncate the INTER_TMDB_DATA table"
        exit 1
    fi

STEP_NUMBER='3'
    SF_QUERY_TAG="insert_data_into_INTER_TABLE_${STEP_NUMBER}"
    echo "Insert Data into INTER_TMDB_DATA table $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c $PROJECT_CONNECTION_NAME -f $SQL_DIR/tmdb_Inter_table.sql -o exit_on_error=true -D "SF_QUERY_TAG=${SF_QUERY_TAG}"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to load data into INTER_tmdb_DATA table"
        exit 1
    fi

STEP_NUMBER='4'
    SF_QUERY_TAG="truncate_from_final_tmdb_data_${STEP_NUMBER}"
    echo "Truncate FINAL_TMDB_DATA table $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c $PROJECT_CONNECTION_NAME -q "TRUNCATE TABLE myDb.mySchema.FINAL_TMDB_DATA" -o exit_on_error=true -D "SF_QUERY_TAG=${SF_QUERY_TAG}"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to Truncate the FINAL_TMDB_DATA table"
        exit 1
    fi

STEP_NUMBER='5'
    SF_QUERY_TAG="insert_data_into_FINAL_TMDB_TABLE_${STEP_NUMBER}"
    echo "Insert Data into FINAL_TMDB_DATA table $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c $PROJECT_CONNECTION_NAME -f $SQL_DIR/tmdb_Final_table.sql -o exit_on_error=true -D "SF_QUERY_TAG=${SF_QUERY_TAG}"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to load data into FINAL_TMDB_DATA table"
        exit 1
    fi
    log_error "Successfully Load into FINAL_TMDB_DATA $(date +%Y%m%d%H%M%S)"