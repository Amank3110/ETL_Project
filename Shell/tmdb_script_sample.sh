#!/bin/bash
. /mnt/c/Users/Aman/Documents/myProject/Properties/myProject_Directory.properties
. /mnt/c/Users/Aman/Documents/myProject/Properties/myProject_Access.properties

LOGFILE="tmdb_script_sample.sh_$(date +%Y%m%d%H%M%S).log"
LOG_PATH="$LOG_DIR/$LOGFILE"

log_error() {
    local step_number=$1
    local error_message=$2
    local timestamp=$(date +%H:%M:%S)
    echo "ERROR Step $step_number $timestamp: $error_message" >> $LOG_PATH
}
#Truncate RAW_TMDB_DATA table
STEP_NUMBER='1'
    SF_QUERY_TAG="Truncate_raw_tmdb_data_${STEP_NUMBER}"
    echo "Truncate table RAW_TMDB_DATA $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c $PROJECT_CONNECTION_NAME -q "TRUNCATE TABLE myDb.mySchema.raw_tmdb_data" -o exit_on_error=true -D "SF_QUERY_TAG=$SF_QUERY_TAG"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to truncate the RAW_TMDB_DATA table"
        exit 1
    fi
#Load Data from S3 to RAW_TMDB_DATA table  
STEP_NUMBER='2'
    SF_QUERY_TAG="Copy_data_into_raw_tmdb_data_${STEP_NUMBER}"
    echo "Copy data From S3 to RAW_TMDB_DATA $(date +%Y%m%d%H%M%S)"
    $SNOWSQL_PATH snowsql -c "$PROJECT_CONNECTION_NAME" -f $SQL_DIR/tmdb_s3ToSnowflake.sql -o exit_on_error=true -D "SF_QUERY_TAG=$SF_QUERY_TAG"

    if [ $? -ne 0 ]
    then
        log_error $STEP_NUMBER "Failed to Load data from S3 to RAW_tmdb_DATA table"
        exit 1
    fi