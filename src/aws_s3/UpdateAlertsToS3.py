import boto3
import datetime
import gzip

from src.data_models.SmartsAlertsDataModel import SmartsAlertsDataModel
from src.utility.Configuration import Configuration


def update_alerts_to_s3(evaluation_date):
    alerts = SmartsAlertsDataModel().initialize(evaluation_date=evaluation_date).evaluate()
    alerts_compress_str = alerts.to_csv(compression='gzip', index=False)
    alerts_gzip_file = gzip.compress(bytes(alerts_compress_str, 'utf-8'))

    config = Configuration().get()['aws_s3']
    access_key_id = [key_id['access_key_id'] for key_id in config if list(key_id.keys())[0] == 'access_key_id'][0]
    secret_access_key = [secret_key['secret_access_key'] for secret_key in config if list(secret_key.keys())[0] == 'secret_access_key'][0]
    bucket = [bucket['bucket_name'] for bucket in config if list(bucket.keys())[0] == 'bucket_name'][0]

    session = boto3.Session(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
    s3 = session.client('s3')

    s3.put_object(Body=alerts_gzip_file, Key='smarts_alerts_{}.csv.gz'.format(evaluation_date), Bucket=bucket)


if __name__ == '__main__':
    update_alerts_to_s3(datetime.date.today() - datetime.timedelta(days=1))
