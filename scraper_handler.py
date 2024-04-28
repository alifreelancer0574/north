import subprocess
import time
import psycopg2
import schedule
import socket
import argparse
import os

def create_connection(db_name, db_user, db_password, db_host):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password)
        print("Database connection established.")
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Failed to connect to database: {error}")
    return conn

def check_db_activity(scraper_id, conn, table_name):
    """ Check the database for activity for a specific scraper """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM {} WHERE scraper_id = %s".format(table_name), (scraper_id,))
            count_now = cur.fetchone()[0]
        return count_now
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database query error: {error}")
        # If the connection is broken, try to re-establish it
        return None

def manage_scrapers(scraper_path, additional_args, conn, table_name):
    global scraper_processes, last_row_counts
    if conn.closed:
        print("Database connection lost. Attempting to reconnect...")
        conn = create_connection(args.db_name, args.db_user, args.db_password, args.db_host)
    for scraper_id in list(scraper_processes.keys()):
        count_now = check_db_activity(scraper_id, conn, table_name)
        if count_now is None or count_now <= last_row_counts[scraper_id]:
            # Restart scraper
            print(f"Restarting scraper {scraper_id}")
            scraper_processes[scraper_id].kill()
            command = ['python3', scraper_path, '--scraper_id', scraper_id] + additional_args
            scraper_processes[scraper_id] = subprocess.Popen(command)
            last_row_counts[scraper_id] = count_now if count_now is not None else 0
        else:
            last_row_counts[scraper_id] = count_now

def start_scraper(scraper_id, scraper_path, additional_args):
    """ Start a scraper process with a unique scraper ID """
    command = ['python3', scraper_path, '--scraper_id', scraper_id] + additional_args
    process = subprocess.Popen(command)
    return process

def parse_args():
    parser = argparse.ArgumentParser(description="Manage multiple scraper processes.")
    parser.add_argument("--num_scrapers", type=int, default=1, help="Number of scraper processes to start (default: 1)")
    parser.add_argument("--interval", type=int, default=3, help="Time interval in minutes for checking scraper activity (default: 3 minutes)")
    parser.add_argument("--scraper_script", type=str, required=True, help="Filename of the scraper script to run")
    parser.add_argument("--scraper_dir", type=str, required=True, help="Directory of the scraper script")
    parser.add_argument("--additional_args", type=str, default="", help="Additional arguments to pass to the scraper script")
    parser.add_argument("--db_name", type=str, required=True, help="Database name to connect to")
    parser.add_argument("--db_user", type=str, required=True, help="Database user")
    parser.add_argument("--db_password", type=str, required=True, help="Database password")
    parser.add_argument("--db_host", type=str, default="localhost", help="Database host (default: localhost)")
    parser.add_argument("--table_name", type=str, required=True, help="Database table to query")
    return parser.parse_args()

# Main execution setup
if __name__ == '__main__':
    args = parse_args()
    machine_name = socket.gethostname()  # Automatically get the hostname
    num_scrapers = args.num_scrapers
    interval = args.interval
    scraper_path = os.path.join(args.scraper_dir, args.scraper_script)
    additional_args = args.additional_args.split()

    # Establish a persistent database connection
    conn = create_connection(args.db_name, args.db_user, args.db_password, args.db_host)

    global scraper_processes, last_row_counts
    scraper_processes = {}
    last_row_counts = {}

    # Start scrapers
    for i in range(num_scrapers):
        id = f"{machine_name}{i}"
        scraper_processes[id] = start_scraper(id, scraper_path, additional_args)
        last_row_counts[id] = 0  # Initialize row count

    # Schedule checks at the specified interval
    if conn is not None:
        schedule.every(interval).minutes.do(manage_scrapers, scraper_path, additional_args, conn, args.table_name)

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)
