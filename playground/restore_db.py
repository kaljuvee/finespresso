import os
import argparse
from urllib.parse import urlparse
import psycopg2
from psycopg2 import extras
import gzip
import io

DATABASE_URL = 'postgresql://finespresso_db_user:XZ0o6UkxcV0poBcLDQf6RGXwEfWmBlnb@dpg-ctj7u2lumphs73f8t9qg-a.frankfurt-postgres.render.com/finespresso_db'

url = urlparse(DATABASE_URL)
db_name = url.path[1:]
user = url.username
password = url.password
host = url.hostname

def parse_sql_dump(content):
    """Parse SQL dump content into statements and COPY data."""
    statements = []
    current_statement = []
    in_copy = False
    copy_statement = None
    copy_data = []
    table_name = None
    columns = None
    
    for line in content.split('\n'):
        line = line.strip()
        if not line or line.startswith('--') or line.startswith('Owner:') or \
           line.startswith('Type:') or line.startswith('Schema:'):
            continue

        if line.startswith('COPY '):
            if current_statement:
                statements.append(' '.join(current_statement))
                current_statement = []
            in_copy = True
            # Extract table name and columns from COPY statement
            copy_parts = line.split('"')
            if len(copy_parts) >= 4:
                table_name = copy_parts[3]  # Get table name
                columns_start = line.find('(')
                columns_end = line.find(')')
                if columns_start > -1 and columns_end > -1:
                    columns = line[columns_start+1:columns_end].replace('"', '')
                    columns = [col.strip() for col in columns.split(',')]
            copy_statement = (table_name, columns)
            continue
        
        if in_copy:
            if line == '\.':
                # End of COPY data
                if copy_statement and copy_data:
                    statements.append(('COPY', copy_statement, copy_data))
                copy_statement = None
                copy_data = []
                in_copy = False
            else:
                copy_data.append(line)
        else:
            current_statement.append(line)
            if line.endswith(';'):
                statements.append(' '.join(current_statement))
                current_statement = []
    
    if current_statement:
        statements.append(' '.join(current_statement))
    
    return statements

def execute_sql_statement(cur, statement, statement_count):
    try:
        if isinstance(statement, tuple) and statement[0] == 'COPY':
            # This is a COPY statement with data
            table_name = statement[1][0]
            columns = statement[1][1]
            data = statement[2]
            
            # Create the COPY command
            copy_sql = f"COPY {table_name} ({','.join(columns)}) FROM STDIN"
            
            # Create StringIO object with the data
            data_io = io.StringIO('\n'.join(data))
            
            # Use copy_expert to load the data
            cur.copy_expert(copy_sql, data_io)
            data_io.close()
        else:
            # Regular SQL statement
            cur.execute(statement)
        return True
    except Exception as e:
        print(f"\nError executing statement #{statement_count}:")
        if isinstance(statement, tuple):
            print(f"COPY to table: {statement[1][0]}")
            print(f"Columns: {statement[1][1]}")
            print(f"First data line preview: {statement[2][0][:200] if statement[2] else 'No data'}...")
        else:
            print(f"Statement preview: {statement[:200]}...")
        print(f"Error details: {str(e)}")
        return False

def restore_backup(backup_file):
    if not backup_file.endswith('.gz'):
        raise ValueError("Unsupported backup file format. Use .sql.gz")

    try:
        print(f"Opening backup file: {backup_file}")
        with gzip.open(backup_file, 'rt', encoding='utf-8') as gz_file:
            sql_content = gz_file.read()
            print(f"Successfully read backup file. Total length: {len(sql_content)} characters")

        # Parse SQL content
        statements = parse_sql_dump(sql_content)
        print(f"Prepared {len(statements)} statements (including COPY commands)")

        print(f"Connecting to database at {host}...")
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cur = conn.cursor()
        print("Database connection established")

        print("Starting database restoration...")
        
        success_count = 0
        error_count = 0
        for i, statement in enumerate(statements, 1):
            print(f"\rProcessing statement {i}/{len(statements)}", end='', flush=True)
            
            if execute_sql_statement(cur, statement, i):
                success_count += 1
            else:
                error_count += 1

        print(f"\nRestore completed:")
        print(f"- Successful statements: {success_count}")
        print(f"- Failed statements: {error_count}")
        print(f"- Total statements processed: {success_count + error_count}")

    except Exception as e:
        print(f"\nCritical error during restoration: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")

def check_database_tables():
    try:
        print("\nChecking database tables...")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cur.fetchall()
        print("Tables in the database:")
        for table in tables:
            print(f"- {table[0]}")
            cur.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cur.fetchone()[0]
            print(f"  Row count: {count}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error checking database tables: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Restore a PostgreSQL backup file.")
    parser.add_argument("backup_file", help="Path to the backup file (.sql.gz)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    try:
        restore_backup(args.backup_file)
        check_database_tables()
    except Exception as e:
        print(f"An error occurred: {e}")
