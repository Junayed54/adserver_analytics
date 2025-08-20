
# from django.core.management.base import BaseCommand
# import pymysql
# from clickhouse_connect import get_client
# from datetime import datetime
# import clickhouse_connect
# class Command(BaseCommand):
#     help = 'Sync data from Revive Adserver MySQL to ClickHouse using clickhouse-connect'

#     def handle(self, *args, **options):
#         # Connect to MySQL
#         try:
#             mysql_conn = pymysql.connect(
                
#                 host='localhost',
#                 user='root',
#                 password='junayed',
#                 database='revive_db',
#                 cursorclass=pymysql.cursors.DictCursor
#             )
#             self.stdout.write(self.style.SUCCESS("✅ Connected to MySQL"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ MySQL connection failed: {e}"))
#             return

#         # Connect to ClickHouse
#         try:
#             clickhouse_client = get_client(
#                 host='localhost',
#                 port=8123,
#                 username='revive_user',
#                 password='revive_pass',
#                 database='revive_db'
#             )
            
#             # clickhouse_client = clickhouse_connect.get_client(host='localhost', username="default")
#             self.stdout.write(self.style.SUCCESS("✅ Connected to ClickHouse"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ ClickHouse connection failed: {e}"))
#             return

#         # Create ClickHouse table
#         try:
#             self.stdout.write("📦 Creating ClickHouse table if not exists...")
#             clickhouse_client.command('''
#                 CREATE TABLE IF NOT EXISTS revive_impressions (
#                     timestamp DateTime,
#                     creative_id UInt32,
#                     impressions UInt32,
#                     clicks UInt32,
#                     zone_id UInt32,
#                     viewer_id String
#                 ) ENGINE = MergeTree()
#                 ORDER BY timestamp
#             ''')
#             self.stdout.write(self.style.SUCCESS("✅ Table ready"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ Failed to create ClickHouse table: {e}"))
#             return

#         # Fetch data from MySQL
#         try:
#             with mysql_conn.cursor() as cursor:
#                 self.stdout.write("📥 Fetching data from MySQL...")
#                 cursor.execute("SELECT * FROM rv_data_bkt_m LIMIT 100")
#                 rows = cursor.fetchall()
#                 self.stdout.write(self.style.SUCCESS(f"✅ Fetched {len(rows)} rows"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ Failed to fetch data: {e}"))
#             return

#         # Prepare and insert data into ClickHouse
#         try:
#             self.stdout.write("🚀 Inserting data into ClickHouse...")
#             data_to_insert = []
#             for row in rows:
#                 data_to_insert.append([
#                     row.get('timestamp', datetime.now()),
#                     int(row.get('creative_id', 0)),
#                     int(row.get('impressions', 0)),
#                     int(row.get('clicks', 0)),
#                     int(row.get('zone_id', 0)),
#                     str(row.get('viewer_id', ''))
#                 ])

#             clickhouse_client.insert(
#                 'revive_impressions',
#                 data_to_insert,
#                 column_names=[
#                     'timestamp', 'creative_id', 'impressions', 'clicks', 'zone_id', 'viewer_id'
#                 ]
#             )
#             self.stdout.write(self.style.SUCCESS("✅ Data inserted successfully"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ Failed to insert data: {e}"))
#         finally:
#             mysql_conn.close()



#  2
# from django.core.management.base import BaseCommand
# import pymysql
# from clickhouse_connect import get_client
# from datetime import datetime

# class Command(BaseCommand):
#     help = 'Sync all MySQL tables to ClickHouse dynamically'

#     def handle(self, *args, **options):
#         # Connect to MySQL
#         try:
#             mysql_conn = pymysql.connect(
#                 host='localhost',
#                 user='root',
#                 password='junayed',
#                 database='revive_db',
#                 cursorclass=pymysql.cursors.DictCursor
#             )
#             mysql_cursor = mysql_conn.cursor()
#             self.stdout.write(self.style.SUCCESS("✅ Connected to MySQL"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ MySQL connection failed: {e}"))
#             return

#         # Connect to ClickHouse
#         try:
#             clickhouse_client = get_client(
#                 host='localhost',
#                 port=8123,
#                 username='revive_user',
#                 password='revive_pass',
#                 database='revive_db'
#             )
#             self.stdout.write(self.style.SUCCESS("✅ Connected to ClickHouse"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ ClickHouse connection failed: {e}"))
#             return

#         # Get all MySQL tables
#         try:
#             mysql_cursor.execute("SHOW TABLES")
#             tables = [row[f'Tables_in_revive_db'] for row in mysql_cursor.fetchall()]
#             self.stdout.write(self.style.SUCCESS(f"📋 Found {len(tables)} tables"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ Failed to fetch table list: {e}"))
#             return

#         # Type mapping function
#         def map_mysql_to_clickhouse(mysql_type):
#             mysql_type = mysql_type.lower()
#             if 'int' in mysql_type:
#                 return 'Int32'
#             elif 'bigint' in mysql_type:
#                 return 'Int64'
#             elif 'float' in mysql_type or 'double' in mysql_type or 'decimal' in mysql_type:
#                 return 'Float64'
#             elif 'datetime' in mysql_type or 'timestamp' in mysql_type:
#                 return 'DateTime'
#             elif 'date' in mysql_type:
#                 return 'Date'
#             else:
#                 return 'String'

#         # Sync each table
#         for table in tables:
#             try:
#                 self.stdout.write(f"\n🔄 Syncing table: {table}")

#                 # Get column definitions
#                 mysql_cursor.execute(f"SHOW COLUMNS FROM {table}")
#                 columns = mysql_cursor.fetchall()
#                 column_defs = []
#                 column_names = []

#                 for col in columns:
#                     name = col['Field']
#                     mysql_type = col['Type']
#                     ch_type = map_mysql_to_clickhouse(mysql_type)
#                     column_defs.append(f"{name} {ch_type}")
#                     column_names.append(name)

#                 # Create ClickHouse table
#                 ch_create = f'''
#                     CREATE TABLE IF NOT EXISTS {table} (
#                         {', '.join(column_defs)}
#                     ) ENGINE = MergeTree()
#                     ORDER BY tuple()
#                 '''
#                 clickhouse_client.command(ch_create)
#                 self.stdout.write(self.style.SUCCESS(f"✅ Created table {table} in ClickHouse"))

#                 # Fetch data from MySQL
#                 mysql_cursor.execute(f"SELECT * FROM {table}")
#                 rows = mysql_cursor.fetchall()
#                 if not rows:
#                     self.stdout.write("⚠️ No data to insert")
#                     continue

#                 # Prepare and insert data
#                 data = []
#                 for row in rows:
#                     data.append([row.get(col, None) for col in column_names])

#                 clickhouse_client.insert(table, data, column_names=column_names)
#                 self.stdout.write(self.style.SUCCESS(f"🚀 Inserted {len(rows)} rows into {table}"))

#             except Exception as e:
#                 self.stdout.write(self.style.ERROR(f"❌ Error syncing table {table}: {e}"))

#         mysql_conn.close()
#         self.stdout.write(self.style.SUCCESS("\n🎉 Sync complete!"))



# # this was worked in locally

# from django.core.management.base import BaseCommand
# import pymysql
# from clickhouse_connect import get_client
# from datetime import datetime

# class Command(BaseCommand):
#     help = 'Sync all MySQL tables to ClickHouse dynamically with Nullable columns'

#     def handle(self, *args, **options):
#         # Connect to MySQL
#         try:
#             # local
#             # mysql_conn = pymysql.connect(
#             #     host='localhost',
#             #     user='root',
#             #     password='junayed',
#             #     database='revive_db',
#             #     cursorclass=pymysql.cursors.DictCursor
#             # )
            
#             # server
#             mysql_conn = pymysql.connect(
#                 host='localhost',
#                 user='re_server_user',
#                 password='re_server_pass',
#                 database='revive',
#                 cursorclass=pymysql.cursors.DictCursor
#             )
#             mysql_cursor = mysql_conn.cursor()
#             self.stdout.write(self.style.SUCCESS("✅ Connected to MySQL"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ MySQL connection failed: {e}"))
#             return

#         # Connect to ClickHouse
#         try:
#             # local
#             # clickhouse_client = get_client(
#             #     host='localhost',
#             #     port=8123,
#             #     username='revive_user',
#             #     password='revive_pass',
#             #     database='revive_db'
#             # )
            
#             # server
#             clickhouse_client = get_client(
#                 host='localhost',
#                 port=8123,
#                 username='default',
#                 password='',
#                 database='re_click_server'
#             )
            
            
#             self.stdout.write(self.style.SUCCESS("✅ Connected to ClickHouse"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ ClickHouse connection failed: {e}"))
#             return

#         # Get all MySQL tables
#         try:
#             mysql_cursor.execute("SHOW TABLES")
#             tables = [row['Tables_in_revive'] for row in mysql_cursor.fetchall()]
#             # tables = [list(row.values())[0] for row in mysql_cursor.fetchall()]
#             self.stdout.write(self.style.SUCCESS(f"📋 Found {len(tables)} tables"))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f"❌ Failed to fetch table list: {e}"))
#             return

#         # Type mapping function
#         def map_mysql_to_clickhouse(mysql_type):
#             mysql_type = mysql_type.lower()
#             if 'int' in mysql_type:
#                 return 'Int32'
#             elif 'bigint' in mysql_type:
#                 return 'Int64'
#             elif 'float' in mysql_type or 'double' in mysql_type or 'decimal' in mysql_type:
#                 return 'Float64'
#             elif 'datetime' in mysql_type or 'timestamp' in mysql_type:
#                 return 'DateTime'
#             elif 'date' in mysql_type:
#                 return 'Date'
#             else:
#                 return 'String'

#         # Safe casting function
#         def safe_cast(value, ch_type):
#             if value is None:
#                 return None
#             try:
#                 if 'Int' in ch_type:
#                     return int(value)
#                 elif 'Float' in ch_type:
#                     return float(value)
#                 elif 'DateTime' in ch_type:
#                     return value if isinstance(value, datetime) else datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
#                 else:
#                     return str(value)
#             except:
#                 return None

#         # Sync each table
#         for table in tables:
#             try:
#                 self.stdout.write(f"\n🔄 Syncing table: {table}")

#                 # Get column definitions
#                 mysql_cursor.execute(f"SHOW COLUMNS FROM {table}")
#                 columns = mysql_cursor.fetchall()
#                 column_defs = []
#                 column_names = []
#                 ch_types = []

#                 for col in columns:
#                     name = col['Field']
#                     mysql_type = col['Type']
#                     nullable = col['Null'] == 'YES'
#                     ch_type = map_mysql_to_clickhouse(mysql_type)
#                     ch_type = f"Nullable({ch_type})" if nullable else f"Nullable({ch_type})"  # force all nullable
#                     column_defs.append(f"{name} {ch_type}")
#                     column_names.append(name)
#                     ch_types.append(ch_type)

#                 # Drop and recreate ClickHouse table
#                 clickhouse_client.command(f"DROP TABLE IF EXISTS {table}")
#                 ch_create = f'''
#                     CREATE TABLE {table} (
#                         {', '.join(column_defs)}
#                     ) ENGINE = MergeTree()
#                     ORDER BY tuple()
#                 '''
#                 clickhouse_client.command(ch_create)
#                 self.stdout.write(self.style.SUCCESS(f"✅ Created table {table} in ClickHouse"))

#                 # Fetch data from MySQL
#                 mysql_cursor.execute(f"SELECT * FROM {table}")
#                 rows = mysql_cursor.fetchall()
#                 if not rows:
#                     self.stdout.write("⚠️ No data to insert")
#                     continue

#                 # Prepare and insert data
#                 data = []
#                 for row in rows:
#                     data.append([safe_cast(row.get(col), ch_types[i]) for i, col in enumerate(column_names)])

#                 clickhouse_client.insert(table, data, column_names=column_names)
#                 self.stdout.write(self.style.SUCCESS(f"🚀 Inserted {len(rows)} rows into {table}"))

#             except Exception as e:
#                 self.stdout.write(self.style.ERROR(f"❌ Error syncing table {table}: {e}"))

#         mysql_conn.close()
#         self.stdout.write(self.style.SUCCESS("\n🎉 Sync complete!"))



from django.core.management.base import BaseCommand
import pymysql
from datetime import datetime
from clickhouse_connect import Client

class Command(BaseCommand):
    help = 'Sync all MySQL tables to ClickHouse dynamically with Nullable columns'

    def handle(self, *args, **options):
        # Connect to MySQL
        try:
            mysql_conn = pymysql.connect(
                host='localhost',
                user='re_server_user',
                password='re_server_pass',
                database='revive',
                cursorclass=pymysql.cursors.DictCursor
            )
            mysql_cursor = mysql_conn.cursor()
            self.stdout.write(self.style.SUCCESS("✅ Connected to MySQL"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ MySQL connection failed: {e}"))
            return

        # Connect to ClickHouse (HTTP client)
        try:
            clickhouse_client = Client(
                host='localhost',
                port=8123,
                username='default',
                password='',
                database='re_click_server'
            )
            self.stdout.write(self.style.SUCCESS("✅ Connected to ClickHouse"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ ClickHouse connection failed: {e}"))
            return

        # Get all MySQL tables
        try:
            mysql_cursor.execute("SHOW TABLES")
            tables = [row['Tables_in_revive'] for row in mysql_cursor.fetchall()]
            self.stdout.write(self.style.SUCCESS(f"📋 Found {len(tables)} tables"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Failed to fetch table list: {e}"))
            return

        # Type mapping function
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
            elif 'enum' in mysql_type:
                return 'String'
            else:
                return 'String'

        # Safe casting function
        def safe_cast(value, ch_type):
            if value is None:
                return None
            try:
                if 'Int' in ch_type:
                    return int(value)
                elif 'Float' in ch_type:
                    return float(value)
                elif 'DateTime' in ch_type:
                    return value if isinstance(value, datetime) else datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                else:
                    return str(value)
            except:
                return None

        # Batch insert size
        BATCH_SIZE = 5000

        # Sync each table
        for table in tables:
            try:
                self.stdout.write(f"\n🔄 Syncing table: {table}")

                # Get column definitions
                mysql_cursor.execute(f"SHOW COLUMNS FROM `{table}`")
                columns = mysql_cursor.fetchall()
                column_defs = []
                column_names = []
                ch_types = []

                for col in columns:
                    name = col['Field']
                    mysql_type = col['Type']
                    ch_type = map_mysql_to_clickhouse(mysql_type)
                    ch_type = f"Nullable({ch_type})"  # force all nullable
                    column_defs.append(f"`{name}` {ch_type}")
                    column_names.append(name)
                    ch_types.append(ch_type)

                # Drop and recreate ClickHouse table (raw SQL)
                ch_create = f'CREATE TABLE IF NOT EXISTS `{table}` ({", ".join(column_defs)}) ENGINE = MergeTree() ORDER BY tuple()'
                clickhouse_client.command(f"DROP TABLE IF EXISTS `{table}`")
                clickhouse_client.command(ch_create)
                self.stdout.write(self.style.SUCCESS(f"✅ Created table {table} in ClickHouse"))

                # Fetch data from MySQL
                mysql_cursor.execute(f"SELECT * FROM `{table}`")
                rows = mysql_cursor.fetchall()
                if not rows:
                    self.stdout.write("⚠️ No data to insert")
                    continue

                # Prepare and insert data in batches
                data_batch = []
                for idx, row in enumerate(rows, 1):
                    clean_row = [safe_cast(row.get(col_name), ch_types[i]) for i, col_name in enumerate(column_names)]
                    data_batch.append(clean_row)

                    if len(data_batch) >= BATCH_SIZE:
                        clickhouse_client.insert(
                            table=table,
                            data=data_batch,
                            column_names=column_names
                        )
                        data_batch = []

                # Insert remaining rows
                if data_batch:
                    clickhouse_client.insert(
                        table=table,
                        data=data_batch,
                        column_names=column_names
                    )

                self.stdout.write(self.style.SUCCESS(f"🚀 Inserted {len(rows)} rows into {table}"))

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Error syncing table {table}: {e}"))

        mysql_conn.close()
        self.stdout.write(self.style.SUCCESS("\n🎉 Sync complete!"))
