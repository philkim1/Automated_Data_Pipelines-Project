from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id =redshift_conn_id
        self.sql_query=sql_query
        self.table=table
        self.truncate=truncate

    def execute(self, context):
        
        redshift_hook=PostgresHook(self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
        redshift_hook.run(f'INSERT INTO {self.table} {self.sql_query}')
