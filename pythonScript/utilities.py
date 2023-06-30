from configparser import ConfigParser
from datetime import datetime
from s3_transfer import S3Transfer
import os
import io
import json
import requests
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


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

    # Fetch data from an API
    def request_api_data(self):
        try:
            self.feed_config = self.load_feed_config('client.ini')

            if 'headers' in self.feed_config and self.feed_config['headers']:
                headers = json.loads(self.feed_config['headers'])
            else:
                headers = None

            if 'params' in self.feed_config and self.feed_config['params']:
                params = json.loads(self.feed_config['params'])
            else:
                params = None

            if 'url' not in self.feed_config:
                print('Error: No URL found in the configuration')
                return None
            else:
                url = self.feed_config['url']

            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                return response
            else:
                print(f"Error Found: Response code: {response.status_code}")
        except Exception as e:
            print(f"Error Found: {e}")
            return None

    # Find format of fetched data (JSON or CSV)
    def find_api_data_format(self):
        response = self.request_api_data()
        self.feed_config = self.load_feed_config('client.ini')
        if 'key' in self.feed_config:
            key = self.feed_config['key']
        else:
            key = None
        content_type = response.headers.get('Content-Type')
        if 'json' in content_type:
            data = response.json()
            if key is not None and key in data:
                df = pd.json_normalize(data[key])
            else:
                df = pd.DataFrame(data)
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

    # if date_column is None 
    def if_dateColumn_not_exists(self, df, columns, bucket_name, access_key, secret_access_key, default_region):
        try:
            feed_config = self.load_feed_config('client.ini')
            s3_location = feed_config.get('s3_location')
            file_name = f'{self.feed_name}.csv'
            if df is None:
                return None
            csv_file = df[columns].to_csv(index=False).encode('utf-8')
            s3_key = f'{s3_location}{file_name}'
            self.upload_to_s3(bucket_name, access_key, secret_access_key, default_region, key=s3_key, body=csv_file)
        except Exception as e:
            print(f'Error While loading files on s3: {e}')
