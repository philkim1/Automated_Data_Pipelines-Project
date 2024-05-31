from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 truncate="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.table=table
        self.truncate=truncate

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift_hook=PostgresHook(self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
            redshift_hook.run(f'TRUNCATE {self.table}')
        self.log.info(f'Load table {self.table}')
        redshift_hook.run(f'INSERT INTO {self.table}{self.sql_query}')
