from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshiftConnId="",
                 sqlStatement="",
                 isIncremental=False,
                 tableName = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sqlStatement=sqlStatement
        self.redshiftConnId=redshiftConnId
        self.isIncremental=isIncremental
        self.tableName=tableName

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshiftConnId)
        if self.isIncremental:
            sqlStatement = "INSERT INTO {} {}".format(self.tableName, self.sqlStatement)
            redshift.run(sqlStatement)
        else:
            sqlStatement = "TRUNCATE TABLE  {}".format(self.tableName)
            redshift.run(sqlStatement)
            sqlStatement = "INSERT INTO {} {}".format(self.tableName, self.sqlStatement)
            redshift.run(sqlStatement)
        self.log.info('LoadDimensionOperator is Implemented')
