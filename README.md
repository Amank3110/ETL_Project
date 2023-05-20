Hey Guys, Thanks for being here 
So In my currect branch that is my complete prject which is defined as ETL(Extract Transform and Load) project in short. In Below I've mentioned the complete description of my project.

ETL Pipeline for Data Extraction and Loading
- Developed an ETL pipeline to extract data from an API, transform it into a desired format, and load it into an S3 bucket. Utilized Python, Pandas, Boto3, and AWS services. The pipeline is designed to split the data by unique dates and save each date's data into a separate CSV file before uploading it to S3.
 
Data Loading to Snowflake from S3
- Built a shell script to load data from an S3 bucket into Snowflake tables. The script leverages the S3 bucket created in the previous project and uses Snowflake's COPY INTO command to load the data. The shell script is executed from a main script that orchestrates the overall ETL process.
