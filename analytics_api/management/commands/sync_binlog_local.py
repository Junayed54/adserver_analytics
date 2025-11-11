# # this was worked in server
# from django.core.management.base import BaseCommand
# from pymysqlreplication import BinLogStreamReader
# from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
# from datetime import datetime
# from queue import Queue
# from threading import Thread
# import time
# import pymysql
# import clickhouse_connect

# class Command(BaseCommand):
#     help = 'Sync MySQL binlog to ClickHouse using pipelined inserts (safe schema)'

#     def handle(self, *args, **kwargs):
#         # Connect to MySQL for schema lookup # server
#         mysql_conn = pymysql.connect(
#             host='localhost',
#             user='revive_user',
#             password='revive_pass',
#             database='revive_db',
#             cursorclass=pymysql.cursors.DictCursor
#         )
        
#         # mysql_conn = pymysql.connect(
#         #     host='161.97.141.58',
#         #     user='re_server_user',
#         #     password='re_server_pass',
#         #     database='revive_db',
#         #     cursorclass=pymysql.cursors.DictCursor
            
#         # )
#         mysql_cursor = mysql_conn.cursor()

#         # # Connect to ClickHouse server
#         # client = clickhouse_connect.get_client(
#         #     host='localhost',
#         #     username='default',
#         #     password='',
#         #     database='re_click_server'
#         # )
        
#         client = clickhouse_connect.get_client(
#             host='localhost',
#             port=8123,
#             username='revive_user',
#             password='revive_pass',
#             database='revive_db'
#         )
#         self.stdout.write(self.style.SUCCESS("🚀 ClickHouse client connected"))

#         # Build column type map from MySQL
#         table_columns = {}

#         def map_mysql_to_clickhouse(mysql_type):
#             mysql_type = mysql_type.lower()
#             if 'bigint' in mysql_type:
#                 return 'Int64'
#             elif 'int' in mysql_type:
#                 return 'Int32'
#             elif 'float' in mysql_type or 'double' in mysql_type or 'decimal' in mysql_type:
#                 return 'Float64'
#             elif 'datetime' in mysql_type or 'timestamp' in mysql_type:
#                 return 'DateTime'
#             elif 'date' in mysql_type:
#                 return 'Date'
#             else:
#                 return 'String'

#         def sanitize_value(val, ch_type):
#             if val is None:
#                 return "NULL"
#             if 'String' in ch_type or 'DateTime' in ch_type or 'Date' in ch_type:
#                 escaped_val = str(val).replace("'", "\\'")
#                 return f"'{escaped_val}'"

#             return str(val)

#         def get_table_schema(table):
#             if table in table_columns:
#                 return table_columns[table]
#             mysql_cursor.execute(f"SHOW COLUMNS FROM `{table}`")
#             columns = mysql_cursor.fetchall()
#             column_names = [col['Field'] for col in columns]
#             column_types = [map_mysql_to_clickhouse(col['Type']) for col in columns]
#             table_columns[table] = (column_names, column_types)
#             return column_names, column_types

#         # Queue for pipelining
#         event_queue = Queue(maxsize=1000)

#         # Worker thread for ClickHouse inserts
#         def clickhouse_worker():
#             batch_size = 50
#             flush_interval = 2
#             buffers = {}
#             deletes = {}  # buffer deletes separately
#             last_flush = time.time()

#             while True:
#                 try:
#                     table, data, action = event_queue.get(timeout=1)
#                 except:
#                     table, data, action = None, None, None

#                 if table and data:
#                     if action == 'delete':
#                         deletes.setdefault(table, []).append(data)
#                     else:
#                         buffers.setdefault(table, []).append(data)

#                 now = time.time()
#                 if now - last_flush >= flush_interval or any(len(buf) >= batch_size for buf in buffers.values()):
#                     for tbl, rows in buffers.items():
#                         if not rows:
#                             continue
#                         column_names, column_types = get_table_schema(tbl)
#                         values_sql = []
#                         for row in rows:
#                             row_sql = []
#                             for i, col in enumerate(column_names):
#                                 val = sanitize_value(row.get(col), column_types[i])
#                                 row_sql.append(val)
#                             values_sql.append(f"({', '.join(row_sql)})")
#                         try:
#                             # Ensure table exists in ClickHouse
#                             try:
#                                 client.command(f"DESCRIBE TABLE `{tbl}`")
#                             except Exception:
#                                 # Auto-create table from MySQL schema
#                                 create_cols = []
#                                 for col, ch_type in zip(column_names, column_types):
#                                     create_cols.append(f"`{col}` {ch_type}")
#                                 create_sql = f"CREATE TABLE IF NOT EXISTS `{tbl}` ({', '.join(create_cols)}) ENGINE = MergeTree() ORDER BY tuple()"
#                                 client.command(create_sql)
#                                 self.stdout.write(self.style.WARNING(f"⚡ Auto-created table {tbl} in ClickHouse"))

#                             # Insert rows
#                             insert_sql = f"INSERT INTO `{tbl}` ({', '.join(column_names)}) VALUES {', '.join(values_sql)}"
#                             client.command(insert_sql)
#                             self.stdout.write(self.style.SUCCESS(f"✅ Synced {len(rows)} rows to {tbl}"))

#                         except Exception as e:
#                             self.stderr.write(f"❌ Error syncing batch to {tbl}: {e}")

#                         buffers[tbl] = []
                    
#                     # Process deletes
#                     for tbl, rows in deletes.items():
#                         if not rows:
#                             continue

#                         # Get primary key for this table (cached for performance)
#                         if tbl not in table_columns or "pk" not in table_columns[tbl]:
#                             mysql_cursor.execute(f"SHOW KEYS FROM `{tbl}` WHERE Key_name = 'PRIMARY'")
#                             pk_info = mysql_cursor.fetchone()
#                             pk_col = pk_info['Column_name'] if pk_info else None
#                             cols, types = get_table_schema(tbl)
#                             pk_type = None
#                             if pk_col and pk_col in cols:
#                                 pk_index = cols.index(pk_col)
#                                 pk_type = types[pk_index]
#                             table_columns[tbl] = {"columns": cols, "types": types, "pk": pk_col, "pk_type": pk_type}

#                         pk_col = table_columns[tbl].get("pk")
#                         pk_type = table_columns[tbl].get("pk_type")

#                         for row in rows:
#                             if not pk_col or pk_col not in row:
#                                 print(f"⚠️ Skipping delete for {tbl}, no primary key found in row: {row}")
#                                 continue

#                             pk_val = row[pk_col]
#                             # Quote if string/date type
#                             if pk_type and ("String" in pk_type or "Date" in pk_type or "DateTime" in pk_type):
#                                 pk_val = f"'{pk_val}'"

#                             try:
#                                 delete_sql = f"ALTER TABLE `{tbl}` DELETE WHERE {pk_col} = {pk_val}"
#                                 client.command(delete_sql)
#                                 print(f"🗑 Deleted row {pk_col}={pk_val} from {tbl}")
#                             except Exception as e:
#                                 print(f"❌ Error deleting row {pk_col}={row.get(pk_col)} from {tbl}: {e}")

#                         deletes[tbl] = []


                        
#                     last_flush = now

#         # Start worker thread
#         Thread(target=clickhouse_worker, daemon=True).start()

#         # Start binlog stream
#         while True:
#             try:
#                 stream = BinLogStreamReader(
#                     # server
#                     # connection_settings={
#                     #     'host': 'localhost',
#                     #     'user': 'binlog_user',
#                     #     'passwd': 'binlog_pass',
#                     #     'database': 'revive'
#                     # },
                    
#                     # local
#                     connection_settings={
#                         'host': 'localhost',
#                         'user': 'binlog_user',
#                         'passwd': 'binlog_pass',
#                         'database': 'revive_db'
#                     },
#                     server_id=2,
#                     blocking=True,
#                     resume_stream=True,
#                     only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
#                     only_schemas=None
#                 )

#                 self.stdout.write(self.style.SUCCESS("🚀 Binlog stream started"))

#                 for binlogevent in stream:
#                     table = binlogevent.table
#                     self.stdout.write(f"📦 Binlog event detected for table: {table}")

#                     for row in binlogevent.rows:
#                         # Detect event type
#                         if isinstance(binlogevent, DeleteRowsEvent):
#                             data = row.get('values')  # row being deleted
#                             action = 'delete'
#                         elif isinstance(binlogevent, WriteRowsEvent):
#                             data = row.get('values')  # new row
#                             action = 'insert'
#                         elif isinstance(binlogevent, UpdateRowsEvent):
#                             data = row.get('after_values')  # updated row
#                             action = 'update'
#                         else:
#                             continue

#                         if not data:
#                             continue

#                         # Clean datetime values
#                         clean_data = {}
#                         for col, val in data.items():
#                             if isinstance(val, datetime):
#                                 clean_data[col] = val.isoformat()
#                             else:
#                                 clean_data[col] = val

#                         self.stdout.write(
#                             f"➡️ Cleaned row data for {table} ({action}): {clean_data}"
#                         )

#                         try:
#                             event_queue.put((table, clean_data, action), timeout=1)
#                         except Exception as e:
#                             self.stderr.write(
#                                 f"⚠️ Queue full, dropping {action} event for {table}: {e}"
#                             )

#                 stream.close()

#             except Exception as e:
#                 self.stderr.write(f"⚠️ Binlog stream error: {e}, retrying in 5 seconds...")
#                 time.sleep(5)





from django.core.management.base import BaseCommand
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from datetime import datetime
from queue import Queue
from threading import Thread
import time
import pymysql
import clickhouse_connect

class Command(BaseCommand):
    help = 'Sync MySQL binlog to ClickHouse using pipelined inserts (safe schema)'

    def handle(self, *args, **kwargs):
        # --------------------------
        # MySQL connection (schema lookup)
        # --------------------------
        mysql_conn = pymysql.connect(
            host='localhost',
            user='revive_user',
            password='revive_pass',
            database='revive_db',
            cursorclass=pymysql.cursors.DictCursor
        )
        mysql_cursor = mysql_conn.cursor()

        # --------------------------
        # ClickHouse client
        # --------------------------
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='revive_user',
            password='revive_pass',
            database='revive_db'
        )
        self.stdout.write(self.style.SUCCESS("🚀 ClickHouse client connected"))

        # --------------------------
        # Caches and helpers
        # --------------------------
        schema_cache = {}   # table -> (column_names, column_types)
        pk_cache = {}       # table -> (pk_col, pk_type)

        def map_mysql_to_clickhouse(mysql_type):
            t = mysql_type.lower()
            # check bigint first
            if 'bigint' in t:
                return 'Int64'
            if 'int' in t:
                return 'Int32'
            if 'float' in t or 'double' in t or 'decimal' in t:
                return 'Float64'
            if 'datetime' in t or 'timestamp' in t:
                return 'DateTime'
            if 'date' in t:
                return 'Date'
            return 'String'

        def sanitize_value(val, ch_type):
            if val is None:
                return "NULL"
            # DateTime/Date/String should be quoted
            if 'String' in ch_type or 'DateTime' in ch_type or 'Date' in ch_type:
                escaped_val = str(val).replace("'", "\\'")
                return f"'{escaped_val}'"
            return str(val)

        def get_table_schema(table):
            """Return (column_names, column_types)"""
            if table in schema_cache:
                return schema_cache[table]
            mysql_cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            columns = mysql_cursor.fetchall()
            column_names = [col['Field'] for col in columns]
            column_types = [map_mysql_to_clickhouse(col['Type']) for col in columns]
            schema_cache[table] = (column_names, column_types)
            return column_names, column_types

        def get_table_pk(table):
            """Return (pk_col, pk_type) or (None, None)"""
            if table in pk_cache:
                return pk_cache[table]
            mysql_cursor.execute(f"SHOW KEYS FROM `{table}` WHERE Key_name = 'PRIMARY'")
            pk_info = mysql_cursor.fetchone()
            pk_col = pk_info['Column_name'] if pk_info else None
            cols, types = get_table_schema(table)
            pk_type = None
            if pk_col and pk_col in cols:
                pk_index = cols.index(pk_col)
                pk_type = types[pk_index]
            pk_cache[table] = (pk_col, pk_type)
            return pk_col, pk_type

        # --------------------------
        # Event queue and worker
        # --------------------------
        event_queue = Queue(maxsize=5000)

        def clickhouse_worker():
            batch_size = 50
            flush_interval = 2.0
            buffers = {}   # table -> [row_dicts] for insert
            deletes = {}   # table -> [row_dicts] for delete
            last_flush = time.time()

            while True:
                try:
                    table, data, action = event_queue.get(timeout=1)
                except Exception:
                    table, data, action = None, None, None

                if table and data:
                    if action == 'delete':
                        deletes.setdefault(table, []).append(data)
                    elif action == 'insert':
                        buffers.setdefault(table, []).append(data)
                    elif action == 'update':
                        # here data expected to be {"before": {...}, "after": {...}}
                        before = data.get('before')
                        after = data.get('after')
                        if before:
                            deletes.setdefault(table, []).append(before)
                        if after:
                            buffers.setdefault(table, []).append(after)

                now = time.time()
                should_flush = (now - last_flush) >= flush_interval or any(len(buf) >= batch_size for buf in buffers.values()) or any(len(d) > 0 for d in deletes.values())

                if should_flush:
                    # First, do inserts (we can do inserts then deletes, but for UPDATE we want delete then insert;
                    # since update enqueues delete and insert separately here we must ensure deletes for updates are processed first)
                    # To be safe: process deletes first (so updates won't create duplicates), then inserts.
                    # Process deletes
                    for tbl, rows in list(deletes.items()):
                        if not rows:
                            deletes[tbl] = []
                            continue

                        pk_col, pk_type = get_table_pk(tbl)
                        if not pk_col:
                            self.stderr.write(f"⚠️ Skipping deletes for {tbl}: no primary key found")
                            deletes[tbl] = []
                            continue

                        # Ensure table exists (so ALTER TABLE DELETE runs against an existing table)
                        try:
                            client.command(f"DESCRIBE TABLE `{tbl}`")
                        except Exception:
                            # If table doesn't exist, create it (ReplacingMergeTree with PK if possible)
                            cols, types = get_table_schema(tbl)
                            create_cols = [f"`{c}` {t}" for c, t in zip(cols, types)]
                            order_by = f"`{pk_col}`" if pk_col else "tuple()"
                            create_sql = f"CREATE TABLE IF NOT EXISTS `{tbl}` ({', '.join(create_cols)}) ENGINE = ReplacingMergeTree() ORDER BY {order_by}"
                            client.command(create_sql)
                            self.stdout.write(self.style.WARNING(f"⚡ Auto-created table {tbl} in ClickHouse (for delete)"))

                        for row in rows:
                            if pk_col not in row:
                                self.stderr.write(f"⚠️ Skipping delete for {tbl}, pk {pk_col} not in row: {row}")
                                continue
                            pk_val = row[pk_col]
                            if pk_val is None:
                                self.stderr.write(f"⚠️ Skipping delete for {tbl}, pk value is NULL: {row}")
                                continue
                            # Quote pk if necessary
                            pk_ch_type = pk_type or ""
                            if pk_ch_type and ("String" in pk_ch_type or "Date" in pk_ch_type or "DateTime" in pk_ch_type):
                                pk_val_escaped = str(pk_val).replace("'", "\\'")
                                pk_val_sql = f"'{pk_val_escaped}'"
                            else:
                                pk_val_sql = str(pk_val)

                            delete_sql = f"ALTER TABLE `{tbl}` DELETE WHERE `{pk_col}` = {pk_val_sql}"
                            try:
                                client.command(delete_sql)
                                self.stdout.write(self.style.SUCCESS(f"🗑 Deleted {tbl} {pk_col}={pk_val}"))
                            except Exception as e:
                                self.stderr.write(f"❌ Error deleting row {pk_col}={pk_val} from {tbl}: {e}")

                        deletes[tbl] = []

                    # Then process inserts (including update's after-values)
                    for tbl, rows in list(buffers.items()):
                        if not rows:
                            buffers[tbl] = []
                            continue

                        column_names, column_types = get_table_schema(tbl)

                        # Ensure table exists
                        try:
                            client.command(f"DESCRIBE TABLE `{tbl}`")
                        except Exception:
                            # Auto-create table using PK as ORDER BY if available
                            pk_col, _ = get_table_pk(tbl)
                            create_cols = [f"`{c}` {t}" for c, t in zip(column_names, column_types)]
                            order_by = f"`{pk_col}`" if pk_col else "tuple()"
                            create_sql = f"CREATE TABLE IF NOT EXISTS `{tbl}` ({', '.join(create_cols)}) ENGINE = ReplacingMergeTree() ORDER BY {order_by}"
                            try:
                                client.command(create_sql)
                                self.stdout.write(self.style.WARNING(f"⚡ Auto-created table {tbl} in ClickHouse"))
                            except Exception as e:
                                self.stderr.write(f"❌ Error auto-creating table {tbl}: {e}")
                                buffers[tbl] = []
                                continue

                        values_sql = []
                        for row in rows:
                            row_sql = []
                            for i, col in enumerate(column_names):
                                ch_type = column_types[i]
                                val = sanitize_value(row.get(col), ch_type)
                                row_sql.append(val)
                            values_sql.append(f"({', '.join(row_sql)})")

                        insert_sql = f"INSERT INTO `{tbl}` ({', '.join([f'`{c}`' for c in column_names])}) VALUES {', '.join(values_sql)}"
                        try:
                            client.command(insert_sql)
                            self.stdout.write(self.style.SUCCESS(f"✅ Synced {len(rows)} rows to {tbl}"))
                        except Exception as e:
                            # helpful debug output
                            self.stderr.write(f"❌ Error syncing batch to {tbl}: {e}")
                            # print SQL for debugging (avoid printing huge SQL in production)
                            self.stderr.write(f"DEBUG Insert SQL (first 1000 chars): {insert_sql[:1000]}")
                        buffers[tbl] = []

                    last_flush = now

                # small sleep to avoid busy loop (keeps latency low)
                time.sleep(0.01)

        # start worker thread
        Thread(target=clickhouse_worker, daemon=True).start()

        # --------------------------
        # Start binlog stream and push events
        # --------------------------
        while True:
            try:
                stream = BinLogStreamReader(
                    connection_settings={
                        'host': 'localhost',
                        'user': 'binlog_user',
                        'passwd': 'binlog_pass',
                        'database': 'revive_db'
                    },
                    server_id=2,
                    blocking=True,
                    resume_stream=True,
                    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                    only_schemas=None
                )

                self.stdout.write(self.style.SUCCESS("🚀 Binlog stream started"))

                for binlogevent in stream:
                    table = binlogevent.table
                    self.stdout.write(f"📦 Binlog event detected for table: {table}")

                    for row in binlogevent.rows:
                        # Delete event
                        if isinstance(binlogevent, DeleteRowsEvent):
                            data = row.get('values')
                            action = 'delete'
                            if not data:
                                continue

                            # clean datetimes
                            clean_data = {}
                            for col, val in data.items():
                                clean_data[col] = val.isoformat() if isinstance(val, datetime) else val

                            try:
                                event_queue.put((table, clean_data, action), timeout=1)
                            except Exception as e:
                                self.stderr.write(f"⚠️ Queue full, dropping delete event for {table}: {e}")

                        # Insert event
                        elif isinstance(binlogevent, WriteRowsEvent):
                            data = row.get('values')
                            action = 'insert'
                            if not data:
                                continue

                            clean_data = {}
                            for col, val in data.items():
                                clean_data[col] = val.isoformat() if isinstance(val, datetime) else val

                            try:
                                event_queue.put((table, clean_data, action), timeout=1)
                            except Exception as e:
                                self.stderr.write(f"⚠️ Queue full, dropping insert event for {table}: {e}")

                        # Update event -> enqueue delete (before) then insert (after) via single 'update' action
                        elif isinstance(binlogevent, UpdateRowsEvent):
                            before = row.get('before_values') or {}
                            after = row.get('after_values') or {}

                            if not before and not after:
                                continue

                            clean_before = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in before.items()} if before else None
                            clean_after = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in after.items()} if after else None

                            payload = {}
                            if clean_before:
                                payload['before'] = clean_before
                            if clean_after:
                                payload['after'] = clean_after

                            try:
                                event_queue.put((table, payload, 'update'), timeout=1)
                            except Exception as e:
                                self.stderr.write(f"⚠️ Queue full, dropping update event for {table}: {e}")

                        else:
                            continue

                stream.close()

            except Exception as e:
                self.stderr.write(f"⚠️ Binlog stream error: {e}, retrying in 5 seconds...")
                time.sleep(5)
