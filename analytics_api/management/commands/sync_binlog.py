# from django.core.management.base import BaseCommand
# from pymysqlreplication import BinLogStreamReader
# from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent
# import clickhouse_connect

# class Command(BaseCommand):
#     help = 'Sync MySQL binlog to ClickHouse using clickhouse-connect'

#     def handle(self, *args, **kwargs):
#         stream = BinLogStreamReader(
#             connection_settings={
#                 'host': '161.97.141.58',
#                 'user': 'binlog_user',
#                 'passwd': 'binlog_pass'
#             },
#             server_id=2,
#             blocking=True,
#             only_events=[WriteRowsEvent, UpdateRowsEvent],
#             only_schemas=['revive']
#         )

#         # local
#         # client = clickhouse_connect.get_client(
#         #     host='localhost',
#         #     username='revive_user',
#         #     password='revive_pass',
#         #     database='revive_db'
#         # )
        
#         # server
#         client = clickhouse_connect.get_client(
#             host='localhost',
#             username='default',
#             password='',
#             database='re_click_server'
#         )

#         self.stdout.write(self.style.SUCCESS("🚀 Binlog sync started with clickhouse-connect"))

#         for binlogevent in stream:
#             table = binlogevent.table
#             for row in binlogevent.rows:
#                 data = row.get('values') or row.get('after_values')
#                 if data:
#                     try:
#                         client.insert(table, [list(data.values())])
#                         self.stdout.write(f"✅ Synced row to {table}: {data}")
#                     except Exception as e:
#                         self.stderr.write(f"❌ Error syncing to {table}: {e}")



from django.core.management.base import BaseCommand
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent
from datetime import datetime
import time
import clickhouse_connect


class Command(BaseCommand):
    help = 'Sync MySQL binlog to ClickHouse using raw SQL inserts'

    def handle(self, *args, **kwargs):
        # ClickHouse client
        client = clickhouse_connect.get_client(
            host='localhost',
            username='default',
            password='',
            database='re_click_server'
        )

        self.stdout.write(self.style.SUCCESS("🚀 ClickHouse client connected"))

        while True:
            try:
                stream = BinLogStreamReader(
                    connection_settings={
                        'host': '161.97.141.58',
                        'user': 'binlog_user',
                        'passwd': 'binlog_pass',
                        'database': 'revive'
                    },
                    server_id=2,
                    blocking=True,
                    resume_stream=True,
                    only_events=[WriteRowsEvent, UpdateRowsEvent],
                    only_schemas=['revive']
                )

                self.stdout.write(self.style.SUCCESS("🚀 Binlog stream started"))

                for binlogevent in stream:
                    table = binlogevent.table
                    for row in binlogevent.rows:
                        data = row.get('values') or row.get('after_values')
                        if not data:
                            continue

                        # Build raw SQL INSERT
                        columns = list(data.keys())
                        values = []
                        for col in columns:
                            val = data[col]
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, (str, datetime)):
                                safe_val = str(val).replace("'", "\\'")
                                values.append(f"'{safe_val}'")
                            else:
                                values.append(str(val))

                        insert_sql = f"INSERT INTO `{table}` ({', '.join(columns)}) VALUES ({', '.join(values)})"

                        try:
                            client.command(insert_sql)
                            self.stdout.write(f"✅ Synced row to {table}: {data}")
                        except Exception as e:
                            self.stderr.write(f"❌ Error syncing to {table}: {e}")

                stream.close()

            except Exception as e:
                self.stderr.write(f"⚠️ Binlog stream error: {e}, retrying in 5 seconds...")
                time.sleep(5)

