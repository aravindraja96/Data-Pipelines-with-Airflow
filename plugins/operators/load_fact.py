from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshiftConnId="",
                 sqlStatement="",
                 tableName = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshiftConnId=redshiftConnId
        self.sqlStatement=sqlStatement
        self.tableName=tableName
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshiftConnId)
        sqlStatement = "INSERT INTO {} {}".format(self.tableName, self.sqlStatement)
        redshift.run(sqlStatement)
        self.log.info("loading data into {} fact table completed".format(self.tableName))
        
        self.log.info('LoadFactOperator Implemented')
