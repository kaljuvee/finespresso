import os
import subprocess
import argparse
from urllib.parse import urlparse
import psycopg2

DATABASE_URL = 'postgresql://finespresso_db_user:XZ0o6UkxcV0poBcLDQf6RGXwEfWmBlnb@dpg-ctj7u2lumphs73f8t9qg-a.frankfurt-postgres.render.com/finespresso_db'

url = urlparse(DATABASE_URL)
db_name = url.path[1:]
user = url.username
password = url.password
host = url.hostname

def restore_backup(backup_file):
    env = os.environ.copy()
    env['PGPASSWORD'] = password
    
    if backup_file.endswith('.gz'):
        restore_command = f"gunzip -c {backup_file} | psql -h {host} -U {user} -d {db_name} -v ON_ERROR_STOP=1"
        shell = True
    elif backup_file.endswith('.tar'):
        restore_command = f"pg_restore -h {host} -U {user} -d {db_name} -v {backup_file}"
        shell = True
    else:
        raise ValueError("Unsupported backup file format. Use .sql.gz or .tar")

    print(f"Executing command: {restore_command}")
    result = subprocess.run(restore_command, env=env, shell=shell, capture_output=True, text=True)
    
    print("STDOUT:")
    print(result.stdout)
    print("STDERR:")
    print(result.stderr)
    
    if result.returncode != 0:
        print(f"Error during restoration. Return code: {result.returncode}")
    else:
        print("Backup restoration process completed")

def check_database_tables():
    try:
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
    parser.add_argument("backup_file", help="Path to the backup file (.sql.gz or .tar)")
    args = parser.parse_args()

    try:
        restore_backup(args.backup_file)
        check_database_tables()
    except Exception as e:
        print(f"An error occurred: {e}")