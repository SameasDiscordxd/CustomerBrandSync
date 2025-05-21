#!/usr/bin/env python
"""
Google Ads Customer Data Upload Tool

This script automates the upload of customer data to Google Ads for Customer Match audiences,
supporting both full and incremental (delta) uploads with virtual environment management.

Features:
- Self-contained single file (no need for separate launcher scripts)
- Virtual environment auto-setup
- Automatic dependency installation
- Retry logic for handling concurrent modification errors
- Rate limiting to prevent API rate limits
"""

import argparse
import getpass
import hashlib
import logging
import os
import sys
import uuid
import subprocess
import time
import random
from datetime import datetime
from pathlib import Path


def ensure_venv():
    """
    Ensure script runs in a virtual environment.
    Creates one if it doesn't exist and re-executes the script within it.
    """
    # Define virtual environment path
    venv_path = Path("./venv_gads")
    venv_python = venv_path / ("Scripts" if sys.platform == "win32" else "bin") / "python"
    requirements_file = Path("requirements.txt")
    
    # Check if we're already in the virtual environment
    in_venv = sys.prefix == str(venv_path.resolve())
    
    # Output for diagnostics
    if len(sys.argv) > 1 and sys.argv[1] == "--debug-venv":
        print(f"Current Python: {sys.executable}")
        print(f"Virtual env Python: {venv_python}")
        print(f"In virtual env: {in_venv}")
        print(f"sys.prefix: {sys.prefix}")
        print(f"venv path resolved: {venv_path.resolve()}")
        sys.exit(0)
    
    if in_venv:
        return  # Already in the virtual environment
    
    # If virtual environment doesn't exist, create it
    if not venv_path.exists():
        print(f"Creating virtual environment at {venv_path}...")
        
        try:
            subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)
            
            # Create requirements.txt if it doesn't exist
            if not requirements_file.exists():
                with open(requirements_file, "w") as f:
                    f.write("\n".join([
                        "pyodbc>=4.0.39",
                        "phonenumbers>=8.13.0",
                        "google-ads>=23.0.0",
                        "pyyaml>=6.0.0"
                    ]))
                print(f"Created {requirements_file}")
            
            # Install dependencies
            pip_cmd = [str(venv_python), "-m", "pip", "install", "-r", str(requirements_file)]
            print(f"Installing dependencies: {' '.join(pip_cmd)}")
            subprocess.run(pip_cmd, check=True)
            
            print("Virtual environment setup complete.")
        except subprocess.CalledProcessError as e:
            print(f"Error setting up virtual environment: {e}")
            sys.exit(1)
    
    # Re-run the script inside the virtual environment
    if not in_venv:
        print(f"Re-launching script in virtual environment...")
        try:
            # Pass all original arguments to the new process
            os.execv(str(venv_python), [str(venv_python)] + sys.argv)
        except OSError as e:
            print(f"Error launching script in virtual environment: {e}")
            sys.exit(1)


def load_config(config_file=None):
    """Load configuration from YAML file and/or environment variables."""
    config = {
        "LOG_FILE_PATH": "customer_upload_script.log",
        "DB_SERVER": "bbtdatanew.privatelink.westus.cloudapp.azure.com,1401",
        "DB_DATABASE": "Venom",
        "DB_USERNAME": "APIUser",
        "GADS_CUSTOMER_ID": "4018470779",
        "USER_LIST_ID": "9027934773",
        "API_BATCH_SIZE": 2500,       # Reduced from 5000 to avoid API limits
        "API_RETRY_COUNT": 3,         # Number of retries for API failures
        "API_RETRY_DELAY_BASE": 2,    # Base delay for exponential backoff (seconds)
        "API_RATE_LIMIT_DELAY": 0.5,  # Delay between batch uploads (seconds)
    }
    
    # Load from file if specified
    if config_file and os.path.exists(config_file):
        try:
            import yaml
            with open(config_file, 'r') as f:
                file_config = yaml.safe_load(f)
                if isinstance(file_config, dict):
                    config.update(file_config)
        except (ImportError, Exception) as e:
            print(f"Warning: Could not load config from file: {e}")
            try:
                # Try JSON if YAML fails
                import json
                with open(config_file, 'r') as f:
                    config.update(json.load(f))
            except (ImportError, Exception) as e:
                print(f"Warning: Could not load config as JSON either: {e}")
    
    # Override with environment variables
    for key in config:
        env_value = os.environ.get(key)
        if env_value is not None:
            # Convert numeric values
            if isinstance(config[key], (int, float)):
                try:
                    if isinstance(config[key], int):
                        config[key] = int(env_value)
                    else:
                        config[key] = float(env_value)
                except ValueError:
                    print(f"Warning: Could not convert environment variable {key}={env_value} to {type(config[key]).__name__}")
            else:
                config[key] = env_value
    
    return config


def setup_logging(log_file_path):
    """Set up logging to file and console."""
    log_dir = os.path.dirname(log_file_path)
    
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print(f"Warning: Could not create log directory '{log_dir}'. Logging to current directory. Error: {e}")
            log_file_path = os.path.basename(log_file_path)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger("GoogleAdsUploader")


class GoogleAdsUploader:
    """Class to handle Google Ads customer data upload operations."""
    
    def __init__(self, config, run_mode="delta"):
        """
        Initialize the uploader with configuration.
        
        Args:
            config (dict): Configuration parameters
            run_mode (str): "full" or "delta" upload mode
        """
        self.config = config
        self.run_mode = run_mode
        self.logger = setup_logging(config["LOG_FILE_PATH"])
        self.run_id = str(uuid.uuid4())
        self.logger.info(f"Generated unique run ID: {self.run_id}")
        
        # Initialize counters
        self.total_rows_processed = 0
        self.email_processed_count = 0
        self.phone_processed_count = 0
        self.address_processed_count = 0
        self.rows_with_any_id_count = 0
        self.processed_operations = []
        
        # Default values
        self.default_region = "US"
        self.fetch_batch_size = 10000
        self.tracking_id = None
        
        # Initialize clients
        self.db_conn = None
        self.googleads_client = None
    
    def initialize_db_connection(self):
        """Initialize the database connection."""
        # Get password
        db_password = os.environ.get("DB_APIUSER_PASSWORD")
        if not db_password:
            self.logger.warning("DB_APIUSER_PASSWORD environment variable not set.")
            try:
                db_password = getpass.getpass(f"Enter password for database user '{self.config['DB_USERNAME']}': ")
                print("Password received, continuing with script...")
                if not db_password:
                    self.logger.error("FATAL: No database password provided via prompt.")
                    sys.exit(1)
            except Exception as e:
                self.logger.error(f"FATAL: Could not get password interactively: {e}")
                sys.exit(1)
        
        # Build connection string
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.config['DB_SERVER']};"
            f"DATABASE={self.config['DB_DATABASE']};"
            f"UID={self.config['DB_USERNAME']};"
            f"PWD={db_password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=180;"
        )
        
        try:
            import pyodbc
            self.db_conn = pyodbc.connect(conn_str, autocommit=True, timeout=300)
            self.logger.info(f"Connected to database: {self.config['DB_DATABASE']} on {self.config['DB_SERVER']}")
            return True
        except Exception as e:
            self.logger.error(f"FATAL: Database connection failed: {e}")
            return False
    
    def initialize_google_ads_client(self):
        """Initialize the Google Ads client."""
        self.logger.info("Initializing Google Ads Client...")
        try:
            from google.ads.googleads.client import GoogleAdsClient
            self.googleads_client = GoogleAdsClient.load_from_storage()
            self.logger.info("Google Ads Client Initialized Successfully.")
            return True
        except Exception as ex:
            self.logger.exception(f"FATAL: Failed to initialize Google Ads Client: {ex}")
            self.logger.error("Ensure google-ads.yaml exists and has valid credentials/permissions.")
            return False
    
    def fetch_and_process_customer_data(self):
        """Fetch customer data from database and process it for Google Ads upload."""
        if not self.db_conn:
            self.logger.error("Database connection not initialized.")
            return False
        
        # Determine which stored procedure execution mode to use
        if self.run_mode == "full":
            self.logger.info("Executing SPROC in FULL UPLOAD mode (@FullUpload = 1).")
            sql_to_execute = "{CALL dbo.GetNewCustomersForGoogleAds (@FullUpload = 1)}"
        else:
            self.logger.info("Executing SPROC in DELTA UPLOAD mode (@FullUpload = 0).")
            sql_to_execute = "{CALL dbo.GetNewCustomersForGoogleAds (@FullUpload = 0)}"
        
        try:
            with self.db_conn.cursor() as cursor:
                self.logger.info(f"Executing: {sql_to_execute}...")
                cursor.execute(sql_to_execute)
                self.logger.info("Stored procedure executed. Fetching results...")
                
                # Process data in batches to manage memory usage
                while True:
                    customer_data_batch = cursor.fetchmany(self.fetch_batch_size)
                    if not customer_data_batch:
                        break
                    
                    for row in customer_data_batch:
                        self.total_rows_processed += 1
                        self._process_customer_row(row)
            
            self.logger.info(f"\nFinished fetching and processing.")
            self.logger.info(f"  Total rows processed from database: {self.total_rows_processed}")
            self.logger.info(f"  Operations prepared for Google Ads: {len(self.processed_operations)}")
            return True
            
        except Exception as db_ex:
            self.logger.exception(f"FATAL ERROR during database processing: {db_ex}")
            return False
    
    def _process_customer_row(self, row):
        """Process a single customer data row."""
        try:
            # Extract and clean customer data fields
            cust_no = row[0].strip() if row[0] else None
            first_name_raw = row[1].strip() if row[1] else None
            last_name_raw = row[2].strip() if row[2] else None
            contact_guid = row[3]
            email_raw = row[4].strip() if row[4] else None
            phone_raw = row[5].strip() if row[5] else None
            zip_code_raw = row[6].strip() if row[6] else None
            state_code_raw = row[7].strip() if row[7] else None
            
            # Initialize variables for hashed identifiers
            hashed_email, hashed_phone = None, None
            hashed_first_name, hashed_last_name = None, None
            country_code_to_use, postal_code_to_use = None, None
            skip_name_for_address = False
            problem_chars = ['/', '&', '"', ';', ':', '#', '*']
            
            # Process identifiers
            hashed_email = self._process_email(email_raw)
            hashed_phone = self._process_phone(phone_raw)
            
            # Process name fields
            if not first_name_raw or not last_name_raw:
                skip_name_for_address = True
            else:
                hashed_first_name, name_valid = self._process_name(first_name_raw, problem_chars)
                if not name_valid:
                    skip_name_for_address = True
                
                hashed_last_name, name_valid = self._process_name(last_name_raw, problem_chars)
                if not name_valid:
                    skip_name_for_address = True
            
            # Process location data
            if state_code_raw:
                state_processed = state_code_raw.strip().upper()
                if len(state_processed) == 2:
                    country_code_to_use = "US"
                    
            if zip_code_raw:
                zip_processed = "".join(filter(str.isdigit, zip_code_raw))
                if len(zip_processed) >= 5:
                    postal_code_to_use = zip_processed[:5]
            
            # Create Google Ads operation if we have valid data
            self._create_google_ads_operation(
                hashed_email, hashed_phone,
                hashed_first_name, hashed_last_name,
                country_code_to_use, postal_code_to_use,
                skip_name_for_address
            )
            
        except Exception as row_ex:
            # Log error but continue processing other rows
            self.logger.error(f"Error processing DB row #{self.total_rows_processed}. Error: {row_ex}")
    
    def _process_email(self, email_raw):
        """Process and hash email address."""
        if not email_raw:
            return None
            
        email_norm = email_raw.lower().strip()
        # Simple email validation: must contain @ and . in domain
        if '@' in email_norm and '.' in email_norm.split('@')[-1]:
            hashed_email = hashlib.sha256(email_norm.encode('utf-8')).hexdigest()
            self.email_processed_count += 1
            return hashed_email
        return None
    
    def _process_phone(self, phone_raw):
        """Process and hash phone number."""
        if not phone_raw:
            return None
            
        digits_only = "".join(filter(str.isdigit, phone_raw))
        if len(digits_only) >= 7:
            try:
                # Parse phone using phonenumbers library (Google's phone lib)
                import phonenumbers
                phone_parsed = phonenumbers.parse(phone_raw, self.default_region)
                if phonenumbers.is_valid_number(phone_parsed):
                    # Format to E.164 format: +[country code][number]
                    phone_e164 = phonenumbers.format_number(phone_parsed, phonenumbers.PhoneNumberFormat.E164)
                    if phone_e164 and phone_e164.startswith('+'):
                        hashed_phone = hashlib.sha256(phone_e164.encode('utf-8')).hexdigest()
                        self.phone_processed_count += 1
                        return hashed_phone
            except Exception:
                pass  # Skip invalid phone numbers silently
        return None
    
    def _process_name(self, name_raw, problem_chars):
        """Process and hash name component."""
        try:
            name_norm = name_raw.lower().strip()
            if any(char in name_norm for char in problem_chars) or len(name_norm) == 0:
                return None, False
            else:
                hashed_name = hashlib.sha256(name_norm.encode('utf-8')).hexdigest()
                return hashed_name, True
        except Exception:
            return None, False
    
    def _create_google_ads_operation(self, hashed_email, hashed_phone, 
                                    hashed_first_name, hashed_last_name,
                                    country_code, postal_code,
                                    skip_name_for_address):
        """Create a Google Ads operation from processed customer data."""
        if not self.googleads_client:
            return
            
        # Create Google Ads user identifiers for available data points
        user_identifiers = []
        
        # Add email identifier if valid
        if hashed_email:
            id_email = self.googleads_client.get_type("UserIdentifier")
            id_email.hashed_email = hashed_email
            user_identifiers.append(id_email)
        
        # Add phone identifier if valid
        if hashed_phone:
            id_phone = self.googleads_client.get_type("UserIdentifier")
            id_phone.hashed_phone_number = hashed_phone
            user_identifiers.append(id_phone)
        
        # Add address identifier if all components are valid
        if not skip_name_for_address and hashed_first_name and hashed_last_name and country_code and postal_code:
            id_address = self.googleads_client.get_type("UserIdentifier")
            address_info = id_address.address_info
            address_info.hashed_first_name = hashed_first_name
            address_info.hashed_last_name = hashed_last_name
            address_info.country_code = country_code
            address_info.postal_code = postal_code
            user_identifiers.append(id_address)
            self.address_processed_count += 1
        
        # Create operation if we have at least one valid identifier
        if user_identifiers:
            self.rows_with_any_id_count += 1
            user_data = self.googleads_client.get_type("UserData")
            user_data.user_identifiers.extend(user_identifiers)
            operation = self.googleads_client.get_type("OfflineUserDataJobOperation")
            operation.create = user_data
            self.processed_operations.append(operation)
    
    def upload_to_google_ads(self):
        """Upload processed operations to Google Ads."""
        if not self.processed_operations:
            self.logger.info("No operations generated from database data. Skipping API upload.")
            return True
            
        if not self.googleads_client:
            self.logger.error("Google Ads client not initialized.")
            return False
            
        self.logger.info(f"\nStarting Google Ads API upload to list ID {self.config['USER_LIST_ID']}...")
        
        # Import Google Ads types
        from google.ads.googleads.v19.enums.types.offline_user_data_job_type import OfflineUserDataJobTypeEnum
        from google.ads.googleads.errors import GoogleAdsException
        
        offline_user_data_job_service = self.googleads_client.get_service("OfflineUserDataJobService")
        user_list_resource_name = self.googleads_client.get_service("UserListService").user_list_path(
            self.config['GADS_CUSTOMER_ID'], self.config['USER_LIST_ID']
        )
        
        # Create the Customer Match upload job
        offline_user_data_job = self.googleads_client.get_type("OfflineUserDataJob")
        offline_user_data_job.type_ = OfflineUserDataJobTypeEnum.OfflineUserDataJobType.CUSTOMER_MATCH_USER_LIST
        offline_user_data_job.customer_match_user_list_metadata.user_list = user_list_resource_name
        
        try:
            # Create the job in Google Ads
            self.logger.info("Creating OfflineUserDataJob...")
            create_job_response = offline_user_data_job_service.create_offline_user_data_job(
                customer_id=self.config['GADS_CUSTOMER_ID'], job=offline_user_data_job
            )
            offline_user_data_job_resource_name = create_job_response.resource_name
            self.logger.info(f"Created OfflineUserDataJob: {offline_user_data_job_resource_name}")
            
            # Upload operations in batches to avoid API limits
            api_batch_size = int(self.config['API_BATCH_SIZE'])
            operation_batches = [
                self.processed_operations[i:i + api_batch_size]
                for i in range(0, len(self.processed_operations), api_batch_size)
            ]
            self.logger.info(f"Adding {len(self.processed_operations)} operations in {len(operation_batches)} batches...")
            
            # Process each batch with rate limiting and retries
            batch_success_count = 0
            for i, batch in enumerate(operation_batches):
                # Add rate limiting delay between batches
                if i > 0:
                    delay = float(self.config['API_RATE_LIMIT_DELAY'])
                    time.sleep(delay)
                
                # Try to upload with retries
                if self._upload_batch_with_retry(offline_user_data_job_service, 
                                               offline_user_data_job_resource_name,
                                               batch, i, len(operation_batches)):
                    batch_success_count += 1
                
                # Progress report every 10 batches
                if (i + 1) % 10 == 0 or i == 0 or i == len(operation_batches) - 1:
                    self.logger.info(f"Progress: {i+1}/{len(operation_batches)} batches ({((i+1)/len(operation_batches))*100:.1f}%)")
            
            # Check if all batches were successful
            if batch_success_count < len(operation_batches):
                self.logger.warning(f"Not all batches were successful: {batch_success_count}/{len(operation_batches)}")
            
            # Run the job after all batches are uploaded
            self.logger.info("Requesting to run the job...")
            run_job_response = offline_user_data_job_service.run_offline_user_data_job(
                resource_name=offline_user_data_job_resource_name
            )
            self.logger.info(f"Job {offline_user_data_job_resource_name} successfully requested to run.")
            
            return batch_success_count > 0  # Success if at least one batch was uploaded
            
        except GoogleAdsException as ex:
            self.logger.exception(f"ERROR during Google Ads API upload: {ex.failure}")
            return False
        except Exception as e:
            self.logger.exception(f"ERROR: An unexpected error occurred during API upload: {e}")
            return False
    
    def _upload_batch_with_retry(self, service, resource_name, batch, batch_index, total_batches):
        """Upload a batch of operations to Google Ads with retry logic."""
        from google.ads.googleads.errors import GoogleAdsException
        
        max_retries = int(self.config['API_RETRY_COUNT'])
        retry_delay_base = float(self.config['API_RETRY_DELAY_BASE'])
        
        for retry in range(max_retries + 1):  # +1 because first attempt is not a retry
            try:
                if retry > 0:
                    # Exponential backoff with jitter
                    delay = retry_delay_base * (2 ** (retry - 1)) + random.uniform(0, 1)
                    self.logger.info(f"Retry {retry}/{max_retries} for batch {batch_index+1}/{total_batches} after {delay:.2f}s delay")
                    time.sleep(delay)
                
                # Prepare and upload the batch
                add_operations_request = self.googleads_client.get_type("AddOfflineUserDataJobOperationsRequest")
                add_operations_request.resource_name = resource_name
                add_operations_request.operations.extend(batch)
                add_operations_request.enable_partial_failure = True
                
                # Upload the batch
                add_operations_response = service.add_offline_user_data_job_operations(
                    request=add_operations_request
                )
                
                # Log the success without showing all batch details
                if batch_index % 10 == 0 or batch_index == 0 or batch_index == total_batches - 1:
                    self.logger.info(f"Added operations from batch {batch_index+1}/{total_batches} to the job.")
                
                # Handle partial failures
                partial_failure_error = getattr(add_operations_response, "partial_failure_error", None)
                if partial_failure_error and getattr(partial_failure_error, "code", 0) != 0:
                    self.logger.warning(f"Partial failures in batch {batch_index+1}. See UI for details.")
                
                return True  # Successful upload
                
            except GoogleAdsException as ex:
                error_msg = str(ex.failure)
                if "CONCURRENT_MODIFICATION" in error_msg and retry < max_retries:
                    self.logger.warning(f"Concurrent modification error on batch {batch_index+1}, will retry")
                    continue  # Will retry
                else:
                    self.logger.error(f"Failed to upload batch {batch_index+1}/{total_batches}: {ex.failure}")
                    return False
                    
            except Exception as e:
                self.logger.error(f"Unexpected error uploading batch {batch_index+1}/{total_batches}: {e}")
                if retry < max_retries:
                    continue  # Will retry
                else:
                    return False
        
        return False  # All retries failed
    
    def update_tracking_record(self, success_flag):
        """Update the tracking record in the database."""
        tracking_description = 'Initial Full Customer Upload' if self.run_mode == "full" else 'Daily Google Ads Customer Upload'
        
        try:
            with self.db_conn.cursor() as cursor:
                # Remove any recent tracking records to prevent duplicates
                cursor.execute("""
                    DELETE FROM dbo.GoogleAdsUploadTracking 
                    WHERE LastUploadDate > DATEADD(minute, -2, GETDATE())
                """)
                
                # Insert one comprehensive record with all metrics
                cursor.execute("""
                    INSERT INTO dbo.GoogleAdsUploadTracking
                        (LastUploadDate, UploadDescription, RowsProcessed, ActualUploadedCount, SuccessFlag)
                    OUTPUT INSERTED.ID
                    VALUES (GETDATE(), ?, ?, ?, ?)
                """, tracking_description, self.total_rows_processed, len(self.processed_operations), success_flag)
                
                id_result = cursor.fetchone()
                self.tracking_id = id_result[0] if id_result else None
                
                if self.tracking_id:
                    self.logger.info(f"Successfully created tracking record with ID: {self.tracking_id}, "
                                    f"RowsProcessed: {self.total_rows_processed}, "
                                    f"ActualUploadedCount: {len(self.processed_operations)}")
                    return True
                else:
                    self.logger.error("Failed to create tracking record")
                    return False
        except Exception as e:
            self.logger.error(f"Error creating final tracking record: {e}")
            return False
    
    def run(self):
        """Run the full upload process."""
        upload_mode_str = "FULL UPLOAD" if self.run_mode == "full" else "DELTA UPLOAD"
        self.logger.info(f"--- Starting Customer Match Upload Script ({upload_mode_str}) ---")
        
        # Initialize connections
        if not self.initialize_db_connection():
            return False
            
        if not self.initialize_google_ads_client():
            return False
            
        # Process data
        if not self.fetch_and_process_customer_data():
            return False
            
        # Upload to Google Ads
        api_upload_success = self.upload_to_google_ads()
        
        # Update tracking record
        final_success_flag = 1 if api_upload_success else 0
        self.update_tracking_record(final_success_flag)
        
        self.logger.info(f"--- SCRIPT FINISHED ({upload_mode_str} - {'SUCCESS' if final_success_flag == 1 else 'FAIL'}) ---")
        return api_upload_success


def setup():
    """Set up the virtual environment."""
    # Create a virtual environment
    venv_path = Path("./venv_gads")
    if venv_path.exists():
        print(f"Virtual environment already exists at {venv_path}")
        choice = input("Do you want to recreate it? (y/n): ")
        if choice.lower() != 'y':
            print("Setup aborted.")
            return
        
        import shutil
        shutil.rmtree(venv_path)
        print(f"Removed existing virtual environment at {venv_path}")
    
    # Create a fresh virtual environment
    try:
        print(f"Creating virtual environment at {venv_path}...")
        subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)
        
        # Install dependencies
        requirements_file = Path("requirements.txt")
        if not requirements_file.exists():
            with open(requirements_file, "w") as f:
                f.write("\n".join([
                    "pyodbc>=4.0.39",
                    "phonenumbers>=8.13.0",
                    "google-ads>=23.0.0",
                    "pyyaml>=6.0.0"
                ]))
            print(f"Created {requirements_file}")
        
        venv_python = venv_path / ("Scripts" if sys.platform == "win32" else "bin") / "python"
        venv_pip = venv_path / ("Scripts" if sys.platform == "win32" else "bin") / "pip"
        
        # Update pip
        print("Updating pip...")
        subprocess.run([str(venv_pip), "install", "--upgrade", "pip"], check=True)
        
        # Install dependencies
        print("Installing dependencies...")
        subprocess.run([str(venv_pip), "install", "-r", str(requirements_file)], check=True)
        
        print("Virtual environment setup complete!")
        print(f"\nTo activate the virtual environment manually:")
        if sys.platform == "win32":
            print(f"    {venv_path}\\Scripts\\activate.bat")
        else:
            print(f"    source {venv_path}/bin/activate")
    
    except subprocess.CalledProcessError as e:
        print(f"Error setting up virtual environment: {e}")
        return False
    
    return True


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Google Ads Customer Data Upload Tool')
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Setup command
    setup_parser = subparsers.add_parser("setup", help="Set up virtual environment and install dependencies")
    
    # Run command with options
    run_parser = subparsers.add_parser("run", help="Run the customer data upload")
    run_parser.add_argument('--full', action='store_true',
                           help='Perform initial full upload by calling SPROC with @FullUpload = 1')
    run_parser.add_argument('--config', type=str, default=None,
                           help='Path to configuration file')
    
    # Debug command
    debug_parser = subparsers.add_parser("debug", help="Show debug information")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Default to "run" if no command specified
    if not args.command:
        args.command = "run"
    
    # Handle setup command
    if args.command == "setup":
        return 0 if setup() else 1
    
    # Handle debug command
    if args.command == "debug":
        print(f"Python executable: {sys.executable}")
        print(f"Python version: {sys.version}")
        print(f"Platform: {sys.platform}")
        try:
            import pyodbc
            print(f"pyodbc version: {pyodbc.version}")
        except ImportError:
            print("pyodbc not installed")
        
        try:
            from google.ads.googleads import client
            print(f"google-ads installed")
        except ImportError:
            print("google-ads not installed")
        
        return 0
    
    # For run command, ensure we're in a virtual environment
    ensure_venv()
    
    # Now run the actual process in the virtual environment
    config = load_config(args.config if hasattr(args, 'config') else None)
    run_mode = "full" if hasattr(args, 'full') and args.full else "delta"
    uploader = GoogleAdsUploader(config, run_mode)
    success = uploader.run()
    
    # Exit with appropriate code
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())