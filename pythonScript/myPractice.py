from datetime import datetime
from utilities import Utilities
import pandas as pd
import sys

class FeedDownloader:
    def __init__(self, feed_name):
        self.feed_name = feed_name
        self.utilityObj = Utilities(self.feed_name)
        self.feed_config = self.utilityObj.load_feed_config('client.ini')
        self.system_config = self.utilityObj.load_system_config('config.ini')

        # AWS configuration
        self.s3_bucket_name = self.system_config['s3_bucket_name']
        self.aws_default_region = self.system_config['aws_default_region']
        self.aws_access_key = self.system_config['aws_access_key']
        self.aws_secret_access_key = self.system_config['aws_secret_access_key']

    def s3_transfer(self):
        try:
            key = self.feed_config['key']
            self.df = self.utilityObj.find_api_data_format(key)
            if self.df is None:
                return None
            columns = self.feed_config['columns'].split(',')
            date_column_name = self.feed_config.get('date_column')
            self.df[date_column_name] = self.df[date_column_name].str[:10]
            self.date_column_format = self.utilityObj.find_date_column_format(self.df, date_column_name)
            if self.date_column_format:
                self.df[date_column_name] = self.df[date_column_name].apply(
                    lambda x: self.utilityObj.change_date_format(x, self.date_column_format, '%Y-%m-%d')
                )
        except Exception as e:
            print(f'Error Found: {e}')
            return None

        try:
            df = self.df[columns]
            if df is None:
                return
            date_column_name = self.feed_config.get('date_column').strip()
            unique_dates = df[date_column_name].str.strip().drop_duplicates()

            for date in unique_dates:
                file_name = datetime.strptime(date, self.date_column_format).strftime('%Y%m%d') + '.csv'
                filtered_df = df[df[date_column_name] == date]

                csv_file = filtered_df.to_csv(index=False).encode('utf-8')
                s3_key = f'{self.feed_name}/{file_name}'
                self.utilityObj.upload_to_s3(
                    bucket_name=self.s3_bucket_name,
                    access_key=self.aws_access_key,
                    secret_access_key=self.aws_secret_access_key,
                    default_region=self.aws_default_region,
                    key=s3_key,
                    body=csv_file
                )
        except Exception as e:
            print(f'Error Found: {e}')
            raise


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide the feed name as an argument.")
        sys.exit(1)

    feed_name = sys.argv[1]
    run_code = FeedDownloader(feed_name)
    run_code.s3_transfer()
