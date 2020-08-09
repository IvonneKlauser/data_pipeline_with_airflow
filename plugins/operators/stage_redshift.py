from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
            table='',
            s3_bucket='',
            s3_key='',
            redshift_conn_id='redshift',
            aws_conn_id='aws_credentials',
            verify=None,
            autocommit=False,
            parameters=None,
            json_path='',
            *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.autocommit = autocommit
        self.parameters = parameters
        self.json_path = json_path
    def execute(self, context):
        self.log.info('StageToRedshiftOperator start')
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=False)
        credentials = self.s3.get_credentials()
        #handle copy commands with json_path and auto formatting separately
        #add json path to ensure that data is copied even if column names are not lower case
        if self.json_path == "auto":
            copy_query = """
                COPY {table}
                FROM 's3://{s3_bucket}/{s3_key}'
                with credentials
                'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                format as json 'auto'
                compupdate off region 'us-west-2'
                TIMEFORMAT AS 'epochmillisecs';
            """.format(
                       table=self.table,
                       s3_bucket=self.s3_bucket,
                       s3_key=self.s3_key,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                      )
        else: 
            copy_query = """
                COPY {table}
                FROM 's3://{s3_bucket}/{s3_key}'
                with credentials
                'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                JSON '{json_path}'
                compupdate off region 'us-west-2'
                TIMEFORMAT AS 'epochmillisecs';
            """.format(
                       table=self.table,
                       s3_bucket=self.s3_bucket,
                       s3_key=self.s3_key,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                       json_path=self.json_path
                      )

        self.log.info('Executing COPY command')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete")
