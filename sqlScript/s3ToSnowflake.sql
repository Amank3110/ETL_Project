COPY INTO myDb.mySchema.RAW_FRIENDS_DATA
FROM @my_stage/client_info/
FILE_FORMAT=(FORMAT_NAME='myFormat', type='csv', field_delimiter=',')
ON_ERROR='abort_statement';