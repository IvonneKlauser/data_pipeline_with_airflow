from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
#from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
            #schema,
            table='',
            s3_bucket='',
            s3_key='',
            redshift_conn_id='redshift',
            aws_conn_id='aws_credentials',
            verify=None,
            #copy_options=tuple(),
            autocommit=False,
            parameters=None,
            json_path='',
            *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        #self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        #self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters
        self.json_path = json_path
    def execute(self, context):
        #check if file format is json, else raise error that only json is supported
        self.log.info('StageToRedshiftOperator start')
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #aws_hook = AwsHook(self.aws_conn_id)
        #credentials = aws_hook.get_credentials()
        #self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        
        #credentials = self.s3.get_credentials()
        #copy_options = '\n\t\t\t'.join(self.copy_options)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=False)
        credentials = self.s3.get_credentials()
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
                       # schema=self.schema,
                       table=self.table,
                       s3_bucket=self.s3_bucket,
                       s3_key=self.s3_key,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                       #copy_options=copy_options
                       #json_path=self.json_path
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
                       # schema=self.schema,
                       table=self.table,
                       s3_bucket=self.s3_bucket,
                       s3_key=self.s3_key,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                       #copy_options=copy_options
                       json_path=self.json_path
                      )

        self.log.info('Executing COPY command...')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")




