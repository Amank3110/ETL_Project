import boto3

class S3Transfer:
    def __init__(self, bucket_name, access_key, secret_access_key, default_region):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_access_key = secret_access_key
        self.default_region = default_region
        self.s3_client = self.get_s3_client()

    def get_s3_client(self):
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.default_region
            )
            return s3_client
        except Exception as e:
            print(f'Error occurred while creating S3 client: {e}')
            raise

    def upload_obj(self, key, body):
        try:
            self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body
            )
            print(f'{key} uploaded to S3 successfully.')
        except Exception as e:
            print(f'Error occurred while uploading to S3: {e}')
            raise
