#!/bin/bash
. /mnt/c/Users/Aman/Documents/myProject/Properties/myProject_Directory.properties
. /mnt/c/Users/Aman/Documents/myProject/Properties/myProject_Access.properties

LOGFILE="myProject_etl_script.sh_$(date +%Y%m%d%H%M%S).log"
LOG_PATH="$LOG_DIR/$LOGFILE"

log_error() {
    local step_number=$1
    local error_message=$2
    local timestamp=$(date +%H:%M:%S)
    echo "ERROR Step $step_number $timestamp: $error_message" >> $LOG_PATH
}

STEP_NUMBER='1'
    bash $SHELL_DIR/myProject_script_sample.sh

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to load data into RAW_FRIENDS_DATA table"
        exit 1
    fi

STEP_NUMBER='2'
    SF_QUERY_TAG="truncate_from_inter_friends_data_${STEP_NUMBER}"
    echo "Truncate INTER_FRIENDS_DATA table $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c $PROJECT_CONNECTION_NAME -q "TRUNCATE TABLE myDb.mySchema.INTER_FRIENDS_DATA" -o exit_on_error=true -D "SF_QUERY_TAG=${SF_QUERY_TAG}"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to Truncate the INTER_FRIENDS_DATA table"
        exit 1
    fi

STEP_NUMBER='3'
    SF_QUERY_TAG="inert_data_into_INTER_TABLE_${STEP_NUMBER}"
    echo "Insert Data into INTER_FRIENDS_DATA table $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c $PROJECT_CONNECTION_NAME -f $SQL_DIR/myProject_Inter_table.sql -o exit_on_error=true -D "SF_QUERY_TAG=${SF_QUERY_TAG}"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to load data into INTER_FRIENDS_DATA table"
        exit 1
    fi

STEP_NUMBER='4'
    SF_QUERY_TAG="truncate_from_final_friends_data_${STEP_NUMBER}"
    echo "Truncate FINAL_FRIENDS_DATA table $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c $PROJECT_CONNECTION_NAME -q "TRUNCATE TABLE myDb.mySchema.FINAL_FRIENDS_DATA" -o exit_on_error=true -D "SF_QUERY_TAG=${SF_QUERY_TAG}"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to Truncate the FINAL_FRIENDS_DATA table"
        exit 1
    fi

STEP_NUMBER='5'
    SF_QUERY_TAG="inert_data_into_FINAL_TABLE_${STEP_NUMBER}"
    echo "Insert Data into FINAL_FRIENDS_DATA table $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c $PROJECT_CONNECTION_NAME -f $SQL_DIR/myProject_Final_table.sql -o exit_on_error=true -D "SF_QUERY_TAG=${SF_QUERY_TAG}"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to load data into FINAL_FRIENDS_DATA table"
        exit 1
    fi
    log_error "Successfully Load into FINAL_FRIENDS_DATA $(date +%Y%m%d%H%M%S)"