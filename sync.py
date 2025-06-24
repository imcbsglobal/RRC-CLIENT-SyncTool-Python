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


# Setup logging with better formatting - clear old log each time
log_file = 'sync.log'
if os.path.exists(log_file):
    os.remove(log_file)  # Remove old log file

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='w'),  # Overwrite log file each time
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

CONFIG_FILE = 'config.json'

# Hard-coded configuration values
HARD_CODED_CONFIG = {
    "table_name": "rrc_clients",
    "target_database": "detector_test_db",
    "sync": {
        "batchSize": 1000
    },
    "database": {
        "username": "DBA",
        "password": "(*$^)"
    }
}


def print_header():
    """Print a nice header for the application"""
    header_msg = "\n" + "=" * 70 + "\n              🚀 OMEGA DATABASE SYNC TOOL 🚀\n" + "=" * 70 + f"\nStarted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n" + "=" * 70 + "\n"
    print(header_msg)
    logger.info("Sync tool started")


def load_config():
    """Load configuration from config.json file - only DSN and API URL are editable"""
    try:
        print("📋 Loading configuration file...")
        logger.info("Loading configuration file...")
        
        with open(CONFIG_FILE, 'r') as f:
            user_config = json.load(f)
        
        # Merge user config (only DSN and API URL) with hard-coded values
        config = HARD_CODED_CONFIG.copy()
        
        # Only allow DSN and API URL from user config
        if 'database' in user_config and 'dsn' in user_config['database']:
            config['database']['dsn'] = user_config['database']['dsn']
        else:
            raise KeyError("DSN not found in configuration")
            
        if 'api' in user_config and 'url' in user_config['api']:
            config['api'] = {'url': user_config['api']['url']}
        else:
            raise KeyError("API URL not found in configuration")
        
        print("✅ Configuration loaded successfully\n")
        logger.info("Configuration loaded successfully")
        return config
        
    except FileNotFoundError:
        error_msg = f"Configuration file '{CONFIG_FILE}' not found!"
        print(f"❌ ERROR: {error_msg}")
        logger.error(error_msg)
        print("   Please ensure config.json exists in the same folder.")
        input("\nPress Enter to exit...")
        sys.exit(1)
    except json.JSONDecodeError:
        error_msg = f"Invalid JSON format in '{CONFIG_FILE}'!"
        print(f"❌ ERROR: {error_msg}")
        logger.error(error_msg)
        print("   Please check your configuration file syntax.")
        input("\nPress Enter to exit...")
        sys.exit(1)
    except KeyError as e:
        error_msg = f"Required configuration missing: {str(e)}"
        print(f"❌ ERROR: {error_msg}")
        logger.error(error_msg)
        print("   Please ensure config.json has 'database.dsn' and 'api.url' fields.")
        input("\nPress Enter to exit...")
        sys.exit(1)


def connect_to_database(config):
    """Connect to SQL Anywhere database using ODBC"""
    try:
        print("🔌 Connecting to database...")
        logger.info("Attempting database connection...")
        
        dsn = config['database']['dsn']
        username = config['database']['username']
        password = config['database']['password']

        print(f"   → DSN: {dsn}")
        print(f"   → User: {username}")
        logger.info(f"Connecting to DSN: {dsn} with user: {username}")

        conn_str = f"DSN={dsn};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)

        print("✅ Database connection successful!\n")
        logger.info("Database connection successful")
        return conn
    except pyodbc.Error as e:
        error_msg = f"Database connection failed: {e}"
        print(f"❌ Database connection failed!")
        print(f"   Error: {e}")
        logger.error(error_msg)
        print("   Please check your database configuration and ensure:")
        print("   • Database server is running")
        print("   • DSN is configured correctly")
        print("   • Username and password are correct")
        input("\nPress Enter to exit...")
        sys.exit(1)


def execute_query(conn, query):
    """Execute SQL query and return results as a list of dictionaries"""
    try:
        logger.info("Executing database query...")
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        cursor.close()
        logger.info(f"Query executed successfully, returned {len(results)} records")
        return results
    except pyodbc.Error as e:
        error_msg = f"Query execution failed: {e}"
        print(f"❌ Query execution failed: {e}")
        logger.error(error_msg)
        return []


def fetch_data(conn, config):
    """Fetch data from the specified table with the given criteria"""
    print("📊 FETCHING DATA FROM DATABASE")
    print("-" * 50)
    logger.info("Starting data fetch from database")

    table_name = config['table_name']

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
    logger.info(f"Fetched {len(results)} records from {table_name}")

    print("-" * 50)
    print(f"📈 TOTAL RECORDS TO SYNC: {len(results):,}")
    print()

    return results


def sync_data_to_api(data, config):
    """Sync data to the API server using the simplified /api/sync endpoint"""
    try:
        api_base_url = config['api']['url']
        table_name = config['table_name']

        print(f"🌐 API Server: {api_base_url}")
        logger.info(f"Starting API sync to: {api_base_url}")
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
            logger.warning("No data to sync")
            return False

        print(f"📦 Syncing {len(data):,} records to {table_name}...")
        logger.info(f"Syncing {len(data)} records to {table_name}")

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
                    logger.info(f"Retry attempt {retry + 1}/3")

                response = requests.post(
                    sync_endpoint,
                    data=json.dumps(payload, cls=DecimalEncoder),
                    headers=headers,
                    timeout=300  # 5 minutes timeout
                )

                print(f"Status: {response.status_code}")
                logger.info(f"API response status: {response.status_code}")

                if response.status_code == 200:
                    response_data = response.json()

                    if response_data.get('success', False):
                        success = True
                        records_processed = response_data.get('records_processed', len(data))
                        success_msg = f"Success! Processed {records_processed} records. Table {table_name} cleared and data inserted"
                        print(f"✅ Success! Processed {records_processed:,} records")
                        print(f"🔥 Table {table_name} cleared and data inserted")
                        logger.info(success_msg)
                        break
                    else:
                        error_msg = response_data.get('error', 'Unknown error')
                        print(f"❌ API Error: {error_msg}")
                        logger.error(f"API Error: {error_msg}")
                else:
                    error_msg = f"HTTP Error {response.status_code}"
                    print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    try:
                        error_data = response.json()
                        print(f"   Error details: {error_data}")
                        logger.error(f"Error details: {error_data}")
                    except:
                        print(f"   Response text: {response.text[:200]}")
                        logger.error(f"Response text: {response.text[:200]}")

                if retry < 2:  # Don't sleep on last attempt
                    print(f"⏳ Retrying in 5 seconds...")
                    time.sleep(5)

            except requests.exceptions.Timeout:
                error_msg = "Timeout error (5 minutes)"
                print(f"⏱️  {error_msg}")
                logger.error(error_msg)
                if retry < 2:
                    print(f"⏳ Retrying in 5 seconds...")
                    time.sleep(5)
            except requests.exceptions.ConnectionError:
                error_msg = f"Connection error - Check if Django server is running at {api_base_url}"
                print(f"🔌 Connection error")
                print(f"   → Check if Django server is running at {api_base_url}")
                logger.error(error_msg)
                if retry < 2:
                    print(f"⏳ Retrying in 5 seconds...")
                    time.sleep(5)
            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                print(f"💥 Unexpected error: {str(e)}")
                logger.error(error_msg)
                if retry < 2:
                    print(f"⏳ Retrying in 5 seconds...")
                    time.sleep(5)

        if not success:
            failure_msg = f"Failed to sync data after 3 attempts. {len(data)} records were not synced"
            print(f"\n❌ Failed to sync data after 3 attempts")
            print(f"   This means {len(data):,} records were not synced")
            logger.error(failure_msg)
            print(f"   Please check:")
            print(f"   • Django server is running at {api_base_url}")
            print(f"   • Database connection is working")
            print(f"   • No firewall blocking the connection")
            return False

        success_msg = f"Sync completed successfully! Total records processed: {len(data)}"
        print(f"🎉 Sync completed successfully!")
        print(f"📊 Total records processed: {len(data):,}")
        logger.info(success_msg)
        print()

        return True

    except Exception as e:
        error_msg = f"Sync Error: {str(e)}"
        print(f"\n❌ Sync Error: {str(e)}")
        logger.error(error_msg)
        import traceback
        traceback_msg = traceback.format_exc()
        print(f"Full traceback:")
        print(traceback_msg)
        logger.error(f"Full traceback: {traceback_msg}")
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
        print(f"   → Table: {config['table_name']}")
        logger.info(f"Config - DSN: {config['database']['dsn']}, API: {config['api']['url']}, Table: {config['table_name']}")
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
        logger.info("Database connection closed")
        print()

        if success:
            success_msg = "SYNC COMPLETED SUCCESSFULLY!"
            print("=" * 70)
            print("           🎉 SYNC COMPLETED SUCCESSFULLY! 🎉")
            print("=" * 70)
            print("✅ All data has been synchronized to the API server")
            print(f"✅ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(success_msg)
            logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70)
            print()
            print("This window will close automatically in 10 seconds...")

            for i in range(3, 0, -1):
                print(f"Closing in {i}...", end="\r", flush=True)
                time.sleep(1)
            sys.exit(0)
        else:
            failure_msg = "SYNC FAILED!"
            print("=" * 70)
            print("            ❌ SYNC FAILED! ❌")
            print("=" * 70)
            logger.error(failure_msg)
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
        interrupt_msg = "Sync cancelled by user"
        print("\n\n⚠️  Sync cancelled by user")
        logger.warning(interrupt_msg)
        input("Press Enter to close...")
        sys.exit(1)
    except Exception as e:
        error_msg = f"UNEXPECTED ERROR: {str(e)}"
        print("\n" + "=" * 70)
        print("            💥 UNEXPECTED ERROR! 💥")
        print("=" * 70)
        print(f"Error: {str(e)}")
        logger.error(error_msg)
        print("\nFull traceback:")
        import traceback
        traceback_msg = traceback.format_exc()
        print(traceback_msg)
        logger.error(f"Full traceback: {traceback_msg}")
        print("\nPlease contact technical support with this error message.")
        print("=" * 70)
        input("\nPress Enter to close...")
        sys.exit(1)


if __name__ == "__main__":
    main()