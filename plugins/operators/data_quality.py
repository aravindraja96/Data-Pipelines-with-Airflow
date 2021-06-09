from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshiftConnId="",
                 dataQualityCheck=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshiftConnId=redshiftConnId
        self.dataQualityCheck=dataQualityCheck


    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshiftConnId)

        for check in self.dataQualityCheck:
            result=check.get('outputSQL')
            accepted=check.get('acceptedValue')
            recordCount=redshift.get_records(result)[0]
            
            if accepted != recordCount[0]:
                self.log.info("Test Failed for SQL "+result)
                raise ValueError('Data Quality Check Failed')
            else:
                self.log.info("Test Passed for SQL "+result)
        
        self.log.info('DataQualityOperator implemented')
                
         
            
