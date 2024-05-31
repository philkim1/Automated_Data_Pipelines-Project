from contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """


    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 json_file="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_file = json_file

    def execute(self, context):
        aws_hook =AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_file
        )

        redshift.run(formatted_sql)





