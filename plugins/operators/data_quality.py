from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_statement = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement

    def execute(self, context):
        self.log.info('DataQualityOperator start')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for (sql_statement, expected_result) in self.sql_statement:
            count = redshift_hook.run(sql_statement)
            if (count != expected_result):
                raise ValueError(f"Data quality check failed for sql statement {sql_statement} with expected result {expected_result}")
                self.log.info('DataQualityOperator failed for sql statement {}'.format(sql_statement))
            else:
                self.log.info('DataQualityOperator finished with statement {}'.format(sql_statement))