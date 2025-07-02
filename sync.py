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

# Hard-coded configuration values - Updated to include all three tables
HARD_CODED_CONFIG = {
    "tables": [
        {
            "name": "rrc_clients",
            "target_table": "rrc_clients"
        },
        {
            "name": "acc_master", 
            "target_table": "acc_master"
        },
        {
            "name": "acc_product",
            "target_table": "acc_product"
        }
    ],
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
    header_msg = "\n" + "=" * 70 + "\n              🚀 SYSMAC DATABASE SYNC TOOL 🚀\n" + "=" * 70 + f"\nStarted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n" + "=" * 70 + "\n"
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


def get_table_query(table_name):
    """Get the appropriate SQL query for each table"""
    queries = {
        "rrc_clients": f'''
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
        ''',
        
        "acc_master": f'''
            SELECT 
              "{table_name}"."code",
              "{table_name}"."name",
              "{table_name}"."super_code",
              "{table_name}"."opening_balance",
              "{table_name}"."debit",
              "{table_name}"."credit",
              "{table_name}"."place",
              "{table_name}"."phone2",
              "{table_name}"."openingdepartment"
            FROM "{table_name}"
            WHERE "{table_name}"."super_code" = 'DEBTO'
        ''',
        
        "acc_product": f'''
            SELECT 
              "{table_name}"."code",
              "{table_name}"."name",
              "{table_name}"."catagory",
              "{table_name}"."unit",
              "{table_name}"."taxcode",
              "{table_name}"."company",
              "{table_name}"."product",
              "{table_name}"."brand",
              "{table_name}"."text6"
            FROM "{table_name}"
        '''
    }
    
    return queries.get(table_name, "")


def fetch_data_from_table(conn, table_name):
    """Fetch data from a specific table"""
    print(f"📊 Fetching data from {table_name}...")
    logger.info(f"Starting data fetch from {table_name}")

    query = get_table_query(table_name)
    
    if not query:
        print(f"❌ No query defined for table: {table_name}")
        logger.error(f"No query defined for table: {table_name}")
        return []

    print(f"   → Executing query for {table_name}...", end=" ", flush=True)

    results = execute_query(conn, query)

    print(f"✅ {len(results):,} records")
    logger.info(f"Fetched {len(results)} records from {table_name}")

    return results


def fetch_all_data(conn, config):
    """Fetch data from all configured tables"""
    print("📊 FETCHING DATA FROM ALL TABLES")
    print("-" * 50)
    logger.info("Starting data fetch from all tables")

    all_data = {}
    total_records = 0

    for table_config in config['tables']:
        table_name = table_config['name']
        target_table = table_config['target_table']
        
        data = fetch_data_from_table(conn, table_name)
        all_data[target_table] = data
        total_records += len(data)

    print("-" * 50)
    print(f"📈 TOTAL RECORDS TO SYNC: {total_records:,}")
    print()

    return all_data


def sync_table_to_api(table_name, data, config):
    """Sync a single table's data to the API server"""
    try:
        api_base_url = config['api']['url']

        headers = {
            'Content-Type': 'application/json'
        }

        # API endpoint
        sync_endpoint = f"{api_base_url}/api/sync"

        if not data:
            print(f"⚠️  No data to sync for {table_name}")
            logger.warning(f"No data to sync for {table_name}")
            return True

        print(f"📦 Syncing {len(data):,} records to {table_name}...")
        logger.info(f"Syncing {len(data)} records to {table_name}")

        # Prepare payload for Django REST API
        payload = {
            "table": table_name,
            "data": data
        }

        print(f"📡 Sending {table_name} data to API...", end=" ", flush=True)

        success = False
        for retry in range(3):  # 3 retries
            try:
                if retry > 0:
                    print(f"\n🔄 Attempt {retry + 1}/3 for {table_name}...", end=" ", flush=True)
                    logger.info(f"Retry attempt {retry + 1}/3 for {table_name}")

                response = requests.post(
                    sync_endpoint,
                    data=json.dumps(payload, cls=DecimalEncoder),
                    headers=headers,
                    timeout=300  # 5 minutes timeout
                )

                print(f"Status: {response.status_code}")
                logger.info(f"API response status for {table_name}: {response.status_code}")

                if response.status_code == 200:
                    response_data = response.json()

                    if response_data.get('success', False):
                        success = True
                        records_processed = response_data.get('records_processed', len(data))
                        success_msg = f"Success! Processed {records_processed} records for {table_name}. Table cleared and data inserted"
                        print(f"✅ Success! Processed {records_processed:,} records")
                        print(f"🔥 Table {table_name} cleared and data inserted")
                        logger.info(success_msg)
                        break
                    else:
                        error_msg = response_data.get('error', 'Unknown error')
                        print(f"❌ API Error for {table_name}: {error_msg}")
                        logger.error(f"API Error for {table_name}: {error_msg}")
                else:
                    error_msg = f"HTTP Error {response.status_code} for {table_name}"
                    print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    try:
                        error_data = response.json()
                        print(f"   Error details: {error_data}")
                        logger.error(f"Error details for {table_name}: {error_data}")
                    except:
                        print(f"   Response text: {response.text[:200]}")
                        logger.error(f"Response text for {table_name}: {response.text[:200]}")

                if retry < 2:  # Don't sleep on last attempt
                    print(f"⏳ Retrying {table_name} in 5 seconds...")
                    time.sleep(5)

            except requests.exceptions.Timeout:
                error_msg = f"Timeout error for {table_name} (5 minutes)"
                print(f"⏱️  {error_msg}")
                logger.error(error_msg)
                if retry < 2:
                    print(f"⏳ Retrying {table_name} in 5 seconds...")
                    time.sleep(5)
            except requests.exceptions.ConnectionError:
                error_msg = f"Connection error for {table_name} - Check if Django server is running at {api_base_url}"
                print(f"🔌 Connection error for {table_name}")
                print(f"   → Check if Django server is running at {api_base_url}")
                logger.error(error_msg)
                if retry < 2:
                    print(f"⏳ Retrying {table_name} in 5 seconds...")
                    time.sleep(5)
            except Exception as e:
                error_msg = f"Unexpected error for {table_name}: {str(e)}"
                print(f"💥 Unexpected error for {table_name}: {str(e)}")
                logger.error(error_msg)
                if retry < 2:
                    print(f"⏳ Retrying {table_name} in 5 seconds...")
                    time.sleep(5)

        if not success:
            failure_msg = f"Failed to sync {table_name} after 3 attempts. {len(data)} records were not synced"
            print(f"\n❌ Failed to sync {table_name} after 3 attempts")
            print(f"   This means {len(data):,} records were not synced for {table_name}")
            logger.error(failure_msg)
            return False

        print(f"🎉 {table_name} sync completed successfully!")
        print()
        return True

    except Exception as e:
        error_msg = f"Sync Error for {table_name}: {str(e)}"
        print(f"\n❌ Sync Error for {table_name}: {str(e)}")
        logger.error(error_msg)
        import traceback
        traceback_msg = traceback.format_exc()
        print(f"Full traceback for {table_name}:")
        print(traceback_msg)
        logger.error(f"Full traceback for {table_name}: {traceback_msg}")
        return False


def sync_all_data_to_api(all_data, config):
    """Sync all tables' data to the API server"""
    try:
        api_base_url = config['api']['url']

        print(f"🌐 API Server: {api_base_url}")
        logger.info(f"Starting API sync to: {api_base_url}")
        print()

        print("📤 SYNCING ALL TABLES TO API")
        print("-" * 50)

        success_count = 0
        total_tables = len(all_data)
        total_records_synced = 0

        for table_name, data in all_data.items():
            print(f"\n📋 Processing table: {table_name}")
            success = sync_table_to_api(table_name, data, config)
            
            if success:
                success_count += 1
                total_records_synced += len(data)
            
            print("-" * 30)

        # Final summary
        print(f"\n📊 SYNC SUMMARY:")
        print(f"   → Tables processed: {total_tables}")
        print(f"   → Tables successful: {success_count}")
        print(f"   → Tables failed: {total_tables - success_count}")
        print(f"   → Total records synced: {total_records_synced:,}")

        if success_count == total_tables:
            success_msg = f"All {total_tables} tables synced successfully! Total records: {total_records_synced}"
            print(f"🎉 All {total_tables} tables synced successfully!")
            logger.info(success_msg)
            return True
        else:
            failure_msg = f"Only {success_count}/{total_tables} tables synced successfully"
            print(f"⚠️  Only {success_count}/{total_tables} tables synced successfully")
            logger.warning(failure_msg)
            return False

    except Exception as e:
        error_msg = f"Overall Sync Error: {str(e)}"
        print(f"\n❌ Overall Sync Error: {str(e)}")
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
        print(f"   → Tables to sync: {len(config['tables'])}")
        for table_config in config['tables']:
            print(f"     • {table_config['name']} → {table_config['target_table']}")
        logger.info(f"Config - DSN: {config['database']['dsn']}, API: {config['api']['url']}, Tables: {len(config['tables'])}")
        print()

        # Connect to database
        conn = connect_to_database(config)

        # Fetch data from all tables
        all_data = fetch_all_data(conn, config)

        # Sync all data to API
        success = sync_all_data_to_api(all_data, config)

        # Close connection
        conn.close()
        print("🔌 Database connection closed")
        logger.info("Database connection closed")
        print()

        if success:
            success_msg = "ALL TABLES SYNC COMPLETED SUCCESSFULLY!"
            print("=" * 70)
            print("           🎉 ALL TABLES SYNC COMPLETED SUCCESSFULLY! 🎉")
            print("=" * 70)
            print("✅ All tables have been synchronized to the API server")
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
            failure_msg = "SOME TABLES SYNC FAILED!"
            print("=" * 70)
            print("            ⚠️  SOME TABLES SYNC FAILED! ⚠️")
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