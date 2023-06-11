COPY INTO myDb.mySchema.RAW_TMDB_DATA
FROM @myStage/tmdb/
FILE_FORMAT=(FORMAT_NAME='myFormat', type='csv', field_delimiter=',')
ON_ERROR='abort_statement';