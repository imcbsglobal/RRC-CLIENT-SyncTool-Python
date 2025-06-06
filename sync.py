import os
import sys
import json
import time
import logging
import requests
import pyodbc
from datetime import datetime, date
from decimal import Decimal


# Custom JSON encoder to handle Decimal objects and date objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, date):
            return obj.isoformat()  # Convert date to ISO format string (YYYY-MM-DD)
        elif isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO format string
        return super(DecimalEncoder, self).default(obj)


# Setup logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',  # Simplified format for command prompt
    handlers=[
        # Overwrite log file each time
        logging.FileHandler('sync.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

CONFIG_FILE = 'config.json'


def print_header():
    """Print a nice header for the application"""
    print("\n" + "=" * 70)
    print("              🚀 OMEGA DATABASE SYNC TOOL 🚀")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")


def load_config():
    """Load configuration from config.json file"""
    try:
        print("📋 Loading configuration file...")
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        print("✅ Configuration loaded successfully\n")
        return config
    except FileNotFoundError:
        print(f"❌ ERROR: Configuration file '{CONFIG_FILE}' not found!")
        print("   Please ensure config.json exists in the same folder.")
        input("\nPress Enter to exit...")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"❌ ERROR: Invalid JSON format in '{CONFIG_FILE}'!")
        print("   Please check your configuration file syntax.")
        input("\nPress Enter to exit...")
        sys.exit(1)


def connect_to_database(config):
    """Connect to SQL Anywhere database using ODBC"""
    try:
        print("🔌 Connecting to database...")
        dsn = config['database']['dsn']
        username = config['database']['username']
        password = config['database']['password']

        print(f"   → DSN: {dsn}")
        print(f"   → User: {username}")

        conn_str = f"DSN={dsn};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)

        print("✅ Database connection successful!\n")
        return conn
    except pyodbc.Error as e:
        print(f"❌ Database connection failed!")
        print(f"   Error: {e}")
        print("   Please check your database configuration and ensure:")
        print("   • Database server is running")
        print("   • DSN is configured correctly")
        print("   • Username and password are correct")
        input("\nPress Enter to exit...")
        sys.exit(1)


def execute_query(conn, query):
    """Execute SQL query and return results as a list of dictionaries"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        cursor.close()
        return results
    except pyodbc.Error as e:
        print(f"❌ Query execution failed: {e}")
        return []


def fetch_data(conn, config):
    """Fetch data from the specified table with the given criteria"""
    print("📊 FETCHING DATA FROM DATABASE")
    print("-" * 50)

    # Get table name from config, default to a common table name if not specified
    table_name = config.get('table_name', 'rrc_clients')

    # Your specified SQL query
    query = f'''
        SELECT 
          "{table_name}"."code",
          "{table_name}"."name",
          "{table_name}"."address",
          "{table_name}"."branch",
          "{table_name}"."district",
          "{table_name}"."state",
          "rrc_product"."name" AS "software",
          "{table_name}"."mobile",
          "{table_name}"."installationdate",
          "{table_name}"."priorty",
          "{table_name}"."directdealing",
          "{table_name}"."rout",
          "{table_name}"."amc",
          "{table_name}"."amcamt",
          "{table_name}"."accountcode",
          "{table_name}"."address3",
          "{table_name}"."lictype",
          "{table_name}"."clients",
          "{table_name}"."sp",
          "{table_name}"."nature"
        FROM "{table_name}"
        LEFT JOIN "rrc_product" ON "{table_name}"."software" = "rrc_product"."code"
        WHERE "{table_name}"."directdealing" IN ('Y','S')
    '''

    print(f"1. Fetching {table_name}...", end=" ", flush=True)

    results = execute_query(conn, query)

    print(f"✅ {len(results):,} records")

    print("-" * 50)
    print(f"📈 TOTAL RECORDS TO SYNC: {len(results):,}")
    print()

    return results


def sync_data_to_api(data, config):
    """Sync data to the API server using the simplified /api/sync endpoint"""
    try:
        api_base_url = config['api']['url']
        table_name = config.get('table_name', 'rrc_clients')

        print(f"🌐 API Server: {api_base_url}")
        print()

        headers = {
            'Content-Type': 'application/json'
        }

        # API endpoint
        sync_endpoint = f"{api_base_url}/api/sync"

        print("📤 SYNCING DATA TO API")
        print("-" * 50)

        if not data:
            print("❌ No data to sync")
            return False

        print(f"📦 Syncing {len(data):,} records to {table_name}...")

        # Prepare payload for Django REST API
        payload = {
            "table": table_name,
            "data": data
        }

        print(f"📡 Sending data to API...", end=" ", flush=True)

        success = False
        for retry in range(3):  # 3 retries
            try:
                if retry > 0:
                    print(f"\n🔄 Attempt {retry + 1}/3...", end=" ", flush=True)

                response = requests.post(
                    sync_endpoint,
                    data=json.dumps(payload, cls=DecimalEncoder),
                    headers=headers,
                    timeout=300  # 5 minutes timeout
                )

                print(f"Status: {response.status_code}")

                if response.status_code == 200:
                    response_data = response.json()

                    if response_data.get('success', False):
                        success = True
                        records_processed = response_data.get(
                            'records_processed', len(data))
                        print(
                            f"✅ Success! Processed {records_processed:,} records")
                        print(
                            f"🔥 Table {table_name} cleared and data inserted")
                        break
                    else:
                        error_msg = response_data.get('error', 'Unknown error')
                        print(f"❌ API Error: {error_msg}")
                else:
                    print(f"❌ HTTP Error {response.status_code}")
                    try:
                        error_data = response.json()
                        print(f"   Error details: {error_data}")
                    except:
                        print(f"   Response text: {response.text[:200]}")

                if retry < 2:  # Don't sleep on last attempt
                    print(f"⏳ Retrying in 5 seconds...")
                    time.sleep(5)

            except requests.exceptions.Timeout:
                print(f"⏱️  Timeout error (5 minutes)")
                if retry < 2:
                    print(f"⏳ Retrying in 5 seconds...")
                    time.sleep(5)
            except requests.exceptions.ConnectionError:
                print(f"🔌 Connection error")
                print(
                    f"   → Check if Django server is running at {api_base_url}")
                if retry < 2:
                    print(f"⏳ Retrying in 5 seconds...")
                    time.sleep(5)
            except Exception as e:
                print(f"💥 Unexpected error: {str(e)}")
                if retry < 2:
                    print(f"⏳ Retrying in 5 seconds...")
                    time.sleep(5)

        if not success:
            print(f"\n❌ Failed to sync data after 3 attempts")
            print(f"   This means {len(data):,} records were not synced")
            print(f"   Please check:")
            print(f"   • Django server is running at {api_base_url}")
            print(f"   • Database connection is working")
            print(f"   • No firewall blocking the connection")
            return False

        print(f"🎉 Sync completed successfully!")
        print(f"📊 Total records processed: {len(data):,}")
        print()

        return True

    except Exception as e:
        print(f"\n❌ Sync Error: {str(e)}")
        import traceback
        print(f"Full traceback:")
        print(traceback.format_exc())
        return False


def main():
    """Main function to run the sync process"""
    try:
        print_header()

        # Load configuration
        config = load_config()

        # Print config summary (without password)
        print("📋 Configuration Summary:")
        print(f"   → Database DSN: {config['database']['dsn']}")
        print(f"   → Database User: {config['database']['username']}")
        print(f"   → API Server: {config['api']['url']}")
        print(f"   → Table: {config.get('table_name', 'rrc_clients')}")
        print()

        # Connect to database
        conn = connect_to_database(config)

        # Fetch data
        data = fetch_data(conn, config)

        # Sync data to API
        success = sync_data_to_api(data, config)

        # Close connection
        conn.close()
        print("🔌 Database connection closed")
        print()

        if success:
            print("=" * 70)
            print("           🎉 SYNC COMPLETED SUCCESSFULLY! 🎉")
            print("=" * 70)
            print("✅ All data has been synchronized to the API server")
            print(
                f"✅ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70)
            print()
            print("This window will close automatically in 10 seconds...")

            for i in range(3, 0, -1):
                print(f"Closing in {i}...", end="\r", flush=True)
                time.sleep(1)
            sys.exit(0)
        else:
            print("=" * 70)
            print("            ❌ SYNC FAILED! ❌")
            print("=" * 70)
            print("Please check the errors above and try again.")
            print("Common solutions:")
            print("• Check internet connection")
            print("• Verify API server is running: python manage.py runserver")
            print("• Check configuration settings")
            print("• Verify Django database is accessible")
            print("• Test API endpoint in browser: http://127.0.0.1:8000/api/")
            print("=" * 70)
            print()
            input("Press Enter to close...")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Sync cancelled by user")
        input("Press Enter to close...")
        sys.exit(1)
    except Exception as e:
        print("\n" + "=" * 70)
        print("            💥 UNEXPECTED ERROR! 💥")
        print("=" * 70)
        print(f"Error: {str(e)}")
        print("\nFull traceback:")
        import traceback
        print(traceback.format_exc())
        print("\nPlease contact technical support with this error message.")
        print("=" * 70)
        input("\nPress Enter to close...")
        sys.exit(1)


if __name__ == "__main__":
    main()
