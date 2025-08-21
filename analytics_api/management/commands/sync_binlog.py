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


# this was worked
# from django.core.management.base import BaseCommand
# from pymysqlreplication import BinLogStreamReader
# from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent
# from datetime import datetime
# import time
# import clickhouse_connect


# class Command(BaseCommand):
#     help = 'Sync MySQL binlog to ClickHouse using raw SQL inserts'

#     def handle(self, *args, **kwargs):
#         # ClickHouse client
#         client = clickhouse_connect.get_client(
#             host='localhost',
#             username='default',
#             password='',
#             database='re_click_server'
#         )

#         self.stdout.write(self.style.SUCCESS("🚀 ClickHouse client connected"))

#         while True:
#             try:
#                 stream = BinLogStreamReader(
#                     connection_settings={
#                         # 'host': '161.97.141.58',
#                         'host': 'localhost',
#                         'user': 'binlog_user',
#                         'passwd': 'binlog_pass',
#                         'database': 'revive'
#                     },
#                     server_id=2,
#                     blocking=True,
#                     resume_stream=True,
#                     only_events=[WriteRowsEvent, UpdateRowsEvent],
#                     only_schemas=['revive']
#                 )

#                 self.stdout.write(self.style.SUCCESS("🚀 Binlog stream started"))

#                 for binlogevent in stream:
#                     table = binlogevent.table
#                     for row in binlogevent.rows:
#                         data = row.get('values') or row.get('after_values')
#                         if not data:
#                             continue

#                         # Build raw SQL INSERT
#                         columns = list(data.keys())
#                         values = []
#                         for col in columns:
#                             val = data[col]
#                             if val is None:
#                                 values.append("NULL")
#                             elif isinstance(val, (str, datetime)):
#                                 safe_val = str(val).replace("'", "\\'")
#                                 values.append(f"'{safe_val}'")
#                             else:
#                                 values.append(str(val))

#                         insert_sql = f"INSERT INTO `{table}` ({', '.join(columns)}) VALUES ({', '.join(values)})"

#                         try:
#                             client.command(insert_sql)
#                             self.stdout.write(f"✅ Synced row to {table}: {data}")
#                         except Exception as e:
#                             self.stderr.write(f"❌ Error syncing to {table}: {e}")

#                 stream.close()

#             except Exception as e:
#                 self.stderr.write(f"⚠️ Binlog stream error: {e}, retrying in 5 seconds...")
#                 time.sleep(5)



from django.core.management.base import BaseCommand
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent
from datetime import datetime
from queue import Queue
from threading import Thread
import time
import pymysql
import clickhouse_connect

class Command(BaseCommand):
    help = 'Sync MySQL binlog to ClickHouse using pipelined inserts (safe schema)'

    def handle(self, *args, **kwargs):
        # Connect to MySQL for schema lookup
        mysql_conn = pymysql.connect(
            host='localhost',
            user='re_server_user',
            password='re_server_pass',
            database='revive',
            cursorclass=pymysql.cursors.DictCursor
        )
        mysql_cursor = mysql_conn.cursor()

        # Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host='localhost',
            username='default',
            password='',
            database='re_click_server'
        )
        self.stdout.write(self.style.SUCCESS("🚀 ClickHouse client connected"))

        # Build column type map from MySQL
        table_columns = {}

        def map_mysql_to_clickhouse(mysql_type):
            mysql_type = mysql_type.lower()
            if 'int' in mysql_type:
                return 'Int32'
            elif 'bigint' in mysql_type:
                return 'Int64'
            elif 'float' in mysql_type or 'double' in mysql_type or 'decimal' in mysql_type:
                return 'Float64'
            elif 'datetime' in mysql_type or 'timestamp' in mysql_type:
                return 'DateTime'
            elif 'date' in mysql_type:
                return 'Date'
            else:
                return 'String'

        def sanitize_value(val, ch_type):
            if val is None:
                return "NULL"
            if 'String' in ch_type or 'DateTime' in ch_type or 'Date' in ch_type:
                return f"'{str(val).replace(\"'\", \"\\'\")}'"
            return str(val)

        def get_table_schema(table):
            if table in table_columns:
                return table_columns[table]
            mysql_cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            columns = mysql_cursor.fetchall()
            column_names = [col['Field'] for col in columns]
            column_types = [map_mysql_to_clickhouse(col['Type']) for col in columns]
            table_columns[table] = (column_names, column_types)
            return column_names, column_types

        # Queue for pipelining
        event_queue = Queue(maxsize=1000)

        # Worker thread for ClickHouse inserts
        def clickhouse_worker():
            batch_size = 50
            flush_interval = 2
            buffers = {}
            last_flush = time.time()

            while True:
                try:
                    table, data = event_queue.get(timeout=1)
                except:
                    table, data = None, None

                if table and data:
                    buffers.setdefault(table, []).append(data)

                now = time.time()
                if now - last_flush >= flush_interval or any(len(buf) >= batch_size for buf in buffers.values()):
                    for tbl, rows in buffers.items():
                        if not rows:
                            continue
                        column_names, column_types = get_table_schema(tbl)
                        values_sql = []
                        for row in rows:
                            row_sql = []
                            for i, col in enumerate(column_names):
                                val = sanitize_value(row.get(col), column_types[i])
                                row_sql.append(val)
                            values_sql.append(f"({', '.join(row_sql)})")
                        try:
                            insert_sql = f"INSERT INTO `{tbl}` ({', '.join(column_names)}) VALUES {', '.join(values_sql)}"
                            client.command(insert_sql)
                            self.stdout.write(self.style.SUCCESS(f"✅ Synced {len(rows)} rows to {tbl}"))
                        except Exception as e:
                            self.stderr.write(f"❌ Error syncing batch to {tbl}: {e}")
                        buffers[tbl] = []
                    last_flush = now

        # Start worker thread
        Thread(target=clickhouse_worker, daemon=True).start()

        # Start binlog stream
        while True:
            try:
                stream = BinLogStreamReader(
                    connection_settings={
                        'host': 'localhost',
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
                    self.stdout.write(f"📦 Binlog event detected for table: {table}")

                    for row in binlogevent.rows:
                        data = row.get('values') or row.get('after_values')
                        if not data:
                            continue

                        clean_data = {}
                        for col, val in data.items():
                            if isinstance(val, datetime):
                                clean_data[col] = val.isoformat()
                            else:
                                clean_data[col] = val

                        self.stdout.write(f"➡️ Cleaned row data for {table}: {clean_data}")

                        try:
                            event_queue.put((table, clean_data), timeout=1)
                        except Exception as e:
                            self.stderr.write(f"⚠️ Queue full, dropping event for {table}: {e}")

                stream.close()

            except Exception as e:
                self.stderr.write(f"⚠️ Binlog stream error: {e}, retrying in 5 seconds...")
                time.sleep(5)
