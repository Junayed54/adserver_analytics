from django.core.management.base import BaseCommand
import pymysql
from datetime import datetime
from clickhouse_connect import get_client

class Command(BaseCommand):
    help = 'Sync all MySQL tables to ClickHouse dynamically with Nullable columns'

    def handle(self, *args, **options):
        # Connect to MySQL
        try:
            mysql_conn = pymysql.connect(
                host='localhost',
                user='revive_user',
                password='revive_pass',
                database='revive_db',
                cursorclass=pymysql.cursors.DictCursor
            )
            mysql_cursor = mysql_conn.cursor()
            self.stdout.write(self.style.SUCCESS("✅ Connected to MySQL"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ MySQL connection failed: {e}"))
            return

        # Connect to ClickHouse
        try:
            clickhouse_client = get_client(
                host='localhost',
                port=8123,
                username='revive_user',
                password='revive_pass',
                database='revive_db'
            )
            self.stdout.write(self.style.SUCCESS("✅ Connected to ClickHouse"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ ClickHouse connection failed: {e}"))
            return

        # Get all MySQL tables
        try:
            mysql_cursor.execute("SHOW TABLES")
            # tables = [row['Tables_in_revive'] for row in mysql_cursor.fetchall()]
            # Use the value of the first key, regardless of its name
            tables = [list(row.values())[0] for row in mysql_cursor.fetchall()]
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
                    ch_type = f"Nullable({ch_type})"
                    column_defs.append(f"`{name}` {ch_type}")
                    column_names.append(name)
                    ch_types.append(ch_type)

                # Drop and create ClickHouse table using raw SQL
                clickhouse_client.command(f"DROP TABLE IF EXISTS `{table}`")
                create_sql = f"CREATE TABLE `{table}` ({', '.join(column_defs)}) ENGINE = MergeTree() ORDER BY tuple()"
                clickhouse_client.command(create_sql)
                self.stdout.write(self.style.SUCCESS(f"✅ Created table {table} in ClickHouse"))

                # Fetch data from MySQL
                mysql_cursor.execute(f"SELECT * FROM `{table}`")
                rows = mysql_cursor.fetchall()
                if not rows:
                    self.stdout.write("⚠️ No data to insert")
                    continue

                # Prepare INSERT values as raw SQL
                values_list = []
                for row in rows:
                    row_values = []
                    for i, col_name in enumerate(column_names):
                        val = safe_cast(row.get(col_name), ch_types[i])
                        if val is None:
                            row_values.append("NULL")
                        elif 'String' in ch_types[i] or 'DateTime' in ch_types[i] or 'Date' in ch_types[i]:
                            # Escape single quotes
                            safe_val = str(val).replace("'", "\\'")
                            row_values.append(f"'{safe_val}'")
                        else:
                            row_values.append(str(val))
                    values_list.append(f"({', '.join(row_values)})")

                # Insert in batches
                for i in range(0, len(values_list), BATCH_SIZE):
                    batch = values_list[i:i+BATCH_SIZE]
                    insert_sql = f"INSERT INTO `{table}` ({', '.join(column_names)}) VALUES {', '.join(batch)}"
                    clickhouse_client.command(insert_sql)

                self.stdout.write(self.style.SUCCESS(f"🚀 Inserted {len(rows)} rows into {table}"))

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Error syncing table {table}: {e}"))

        mysql_conn.close()
        self.stdout.write(self.style.SUCCESS("\n🎉 Sync complete!"))
