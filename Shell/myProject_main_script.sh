#!/bin/bash
. /mnt/c/Users/Aman/Documents/myProject/Properties/myProject_Directory.properties
. /mnt/c/Users/Aman/Documents/myProject/Properties/myProject_Access.properties

LOGFILE="myProject_main_script.sh_$(date +%Y%m%d%H%M%S).log"
LOG_PATH="$LOG_DIR/$LOGFILE"

log_error() {
    local step_number=$1
    local error_message=$2
    local timestamp=$(date +%H:%M:%S)
    echo "ERROR Step $step_number $timestamp: $error_message" >> $LOG_PATH
}

#Extracting Data on S3
STEP_NUMBER='1'
	if [ $# -eq 0 ]; then
		cd $PYTHON_DIR
		echo "Extracting Data Processing:- "
		python3 $PYTHON_DIR/Etl.py
		
		if [[ $# -ne 0 ]]; then
			log_error $STEP_NUMBER "Error while loading on S3"
		fi
		echo "Data Successfully Extracted"
	fi

#Loading Extracted Data on Snowflake
STEP_NUMBER='2'
	if [[ $# -eq 0 ]]; then
		cd $SHELL_DIR
		bash $SHELL_DIR/myProject_etl_script.sh

		if [[ $# -ne 0 ]]; then
			log_error $STEP_NUMBER "Error while loading S3 to Snowflake Tables"
		fi
	fi
