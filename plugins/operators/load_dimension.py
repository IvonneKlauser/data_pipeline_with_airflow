from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_statement = "",
                 target_table = "",
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.target_table = target_table
        self.truncate = truncate

    def execute(self, context):
        self.log.info('LoadDimensionOperator start with table {}'.format(self.target_table))
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)      
        if (self.truncate == True):
            self.log.info('LoadDimensionOperator truncate table {}'.format(self.target_table))
            redshift_hook.run(SqlQueries.truncate_table.format(self.target_table))
        sql_insert = "INSERT INTO {} {}".format(self.target_table,self.sql_statement)
        redshift_hook.run(sql_insert)
        self.log.info('LoadDimensionOperator finished with table {}'.format(self.target_table))
