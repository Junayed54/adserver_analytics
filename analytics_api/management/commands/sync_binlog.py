from django.core.management.base import BaseCommand
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent
import clickhouse_connect

class Command(BaseCommand):
    help = 'Sync MySQL binlog to ClickHouse using clickhouse-connect'

    def handle(self, *args, **kwargs):
        stream = BinLogStreamReader(
            connection_settings={
                'host': '161.97.141.58',
                'user': 'binlog_user',
                'passwd': 'binlog_pass'
            },
            server_id=2,
            blocking=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent],
            only_schemas=['revive']
        )

        # local
        # client = clickhouse_connect.get_client(
        #     host='localhost',
        #     username='revive_user',
        #     password='revive_pass',
        #     database='revive_db'
        # )
        
        # server
        client = clickhouse_connect.get_client(
            host='localhost',
            username='default',
            password='',
            database='re_click_server'
        )

        self.stdout.write(self.style.SUCCESS("🚀 Binlog sync started with clickhouse-connect"))

        for binlogevent in stream:
            table = binlogevent.table
            for row in binlogevent.rows:
                data = row.get('values') or row.get('after_values')
                if data:
                    try:
                        client.insert(table, [list(data.values())])
                        self.stdout.write(f"✅ Synced row to {table}: {data}")
                    except Exception as e:
                        self.stderr.write(f"❌ Error syncing to {table}: {e}")
