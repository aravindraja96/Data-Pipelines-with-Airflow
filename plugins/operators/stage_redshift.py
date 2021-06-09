from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    stagingCopyStatement=""" 
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}'
    JSON '{}'
    COMPUPDATE OFF;
    """

    @apply_defaults
    def __init__(self,
                 redshiftConnId="",
                 awsCredentialsId="",
                 table="",
                 s3Bucket="",
                 s3Key="",
                 region="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshiftConnId=redshiftConnId
        self.awsCredentialsId=awsCredentialsId
        self.table=table
        self.s3Bucket=s3Bucket
        self.s3Key=s3Key
        self.region=region
        self.json=json
        
        
        

    def execute(self, context):
        awsHook=AwsHook(self.awsCredentialsId)
        credentials=awsHook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshiftConnId)
        s3Path = "s3://{}/{}".format(self.s3Bucket, self.s3Key)
        
        copyStatementGeneration = StageToRedshiftOperator.stagingCopyStatement.format(
            self.table,
            s3Path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json
        )
        
        redshift.run(copyStatementGeneration)
        self.log.info('StageToRedshiftOperator Implemented')





