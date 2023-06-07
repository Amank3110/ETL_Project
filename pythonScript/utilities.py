from configparser import ConfigParser
from api_call import ApiCall
from datetime import datetime
from s3_transfer import S3Transfer
import os
import io
import sys
import pandas as pd


class Utilities:
    def __init__(self, feed_name):
        self.feed_name = feed_name

    # Read feed configuration
    def load_feed_config(self, file_name):
        config = ConfigParser()
        config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'{file_name}')
        config.read(config_file_path)

        if self.feed_name not in config.sections():
            raise ValueError(f"Invalid feed name: {self.feed_name}")

        return dict(config.items(self.feed_name))

    # Read system configuration
    def load_system_config(self, file_name):
        config = ConfigParser()
        config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'{file_name}')
        config.read(config_file_path)

        if 'aws' not in config.sections():
            raise ValueError("AWS configuration is missing in config.ini")

        return dict(config.items('aws'))

    # change Date format
    def change_date_format(self, date_string, from_formats, to_format):
        for from_format in from_formats:
            try:
                date_obj = datetime.strptime(date_string, from_format)
                new_date_string = date_obj.strftime(to_format)
                return new_date_string
            except ValueError:
                pass
        return date_string

    # find date format of the date_column
    def find_date_column_format(self, df, date_column_name):
        date_column_values = df[date_column_name].dropna()
        date_formats = ['%Y-%m-%d', '%m-%d-%Y', '%d-%m-%Y', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']

        if len(date_column_values) > 0:
            for i in range(len(date_formats)):
                try:
                    pd.to_datetime(date_column_values, format=date_formats[i])
                    return date_formats[i]
                except ValueError:
                    pass
        return None

    # split files based on common dates
    def split_by_date(self, file_name, date_column):
        file_path = file_name
        date = date_column
        df = pd.read_csv(file_path, parse_dates=[date], infer_datetime_format=True)
        for i, j in df.groupby(date_column):
            split_file_name = i.replace('[/-]', '', regex=True)
            csv_file = j.to_csv(f'{split_file_name}.csv', index=False, compression='csv')

    # find format of fetched data (json or csv)
    def find_api_data_format(self, key):
        apiFileObj = ApiCall(self.feed_name)
        response = apiFileObj.request_api_data()
        content_type = response.headers['Content-Type']
        if 'json' in content_type:
            data = response.json()
            df = pd.DataFrame(data[key])
            return df
        else:
            data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(data))
            return df
        
    # upload files on s3 bucket    
    def upload_to_s3(self, bucket_name, access_key, secret_access_key, default_region, key, body):
        try:
            s3_transfer = S3Transfer(bucket_name, access_key, secret_access_key, default_region)
            s3_transfer.upload_obj(key, body)
        except Exception as e:
            print(f'Error while loading files on s3: {e}')
