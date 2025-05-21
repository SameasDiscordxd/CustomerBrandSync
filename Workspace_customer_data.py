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
- Brand-specific targeting capabilities
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
        "DEFAULT_USER_LIST_ID": "9027934773",  # Default user list if no brand specified
        "API_BATCH_SIZE": 2500,       # Reduced from 5000 to avoid API limits
        "API_RETRY_COUNT": 3,         # Number of retries for API failures
        "API_RETRY_DELAY_BASE": 2,    # Base delay for exponential backoff (seconds)
        "API_RATE_LIMIT_DELAY": 0.5,  # Delay between batch uploads (seconds)
        # Brand-specific configuration
        "BRANDS": {
            # Default configuration for the main brand
            "default": {
                "USER_LIST_ID": "9027934773",
                "DESCRIPTION": "Default Brand Customer List"
            },
            # Example of additional brand configurations
            # These should be populated from the config file or environment variables
        }
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
        if key == "BRANDS":
            continue  # Skip BRANDS here, process separately
        
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
    
    # Look for brand-specific environment variables (format: BRAND_NAME_USER_LIST_ID)
    for env_key, env_value in os.environ.items():
        parts = env_key.split('_')
        if len(parts) >= 3 and parts[-2] == "USER" and parts[-1] == "LIST":
            brand_name = "_".join(parts[:-2]).lower()
            if brand_name and brand_name != "default":
                if brand_name not in config["BRANDS"]:
                    config["BRANDS"][brand_name] = {
                        "USER_LIST_ID": env_value,
                        "DESCRIPTION": f"{brand_name.title()} Brand Customer List"
                    }
                else:
                    config["BRANDS"][brand_name]["USER_LIST_ID"] = env_value
                    
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
    
    def __init__(self, config, run_mode="delta", brand=None):
        """
        Initialize the uploader with configuration.
        
        Args:
            config (dict): Configuration parameters
            run_mode (str): "full" or "delta" upload mode
            brand (str): Brand to filter customers for, or None for all brands
        """
        self.config = config
        self.run_mode = run_mode
        self.brand = brand
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
        
        # Brand-specific configuration
        self.available_brands = list(config["BRANDS"].keys())
        self.logger.info(f"Available brands: {', '.join(self.available_brands)}")
        
        if brand and brand not in self.available_brands:
            self.logger.warning(f"Specified brand '{brand}' not found in configuration. Using default.")
            self.brand = "default"
        
        # Set user list ID based on brand
        if brand and brand in self.available_brands:
            self.user_list_id = config["BRANDS"][brand]["USER_LIST_ID"]
            self.logger.info(f"Using brand: {brand}, User List ID: {self.user_list_id}")
        else:
            self.user_list_id = config["DEFAULT_USER_LIST_ID"]
            self.logger.info(f"Using default User List ID: {self.user_list_id}")
    
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
            if self.brand and self.brand != "default":
                self.logger.info(f"Filtering data for brand: {self.brand}")
                sql_to_execute = "{CALL dbo.GetNewCustomersForGoogleAdsByBrand (@FullUpload = 1, @Brand = ?)}"
                params = (self.brand,)
            else:
                sql_to_execute = "{CALL dbo.GetNewCustomersForGoogleAds (@FullUpload = 1)}"
                params = ()
        else:
            self.logger.info("Executing SPROC in DELTA UPLOAD mode (@FullUpload = 0).")
            if self.brand and self.brand != "default":
                self.logger.info(f"Filtering data for brand: {self.brand}")
                sql_to_execute = "{CALL dbo.GetNewCustomersForGoogleAdsByBrand (@FullUpload = 0, @Brand = ?)}"
                params = (self.brand,)
            else:
                sql_to_execute = "{CALL dbo.GetNewCustomersForGoogleAds (@FullUpload = 0)}"
                params = ()
        
        try:
            with self.db_conn.cursor() as cursor:
                self.logger.info(f"Executing: {sql_to_execute}...")
                if params:
                    cursor.execute(sql_to_execute, params)
                else:
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
            
            # Check for brand column (added for brand-specific targeting)
            brand_raw = None
            if len(row) > 8:
                brand_raw = row[8].strip().lower() if row[8] else None
            
            # Skip if we're filtering by brand and this row doesn't match
            if self.brand and self.brand != "default" and brand_raw and brand_raw != self.brand:
                return
            
            # Initialize variables for hashed identifiers
            hashed_email, hashed_phone = None, None
            hashed_first_name, hashed_last_name = None, None
            country_code_to_use, postal_code_to_use = None, None
            skip_name_for_address = False
            problem_chars = ['/', '&', '"', ';', ':', '#', '*']
            
            # Process email
            if email_raw and '@' in email_raw and '.' in email_raw:
                try:
                    # Normalize email - convert to lowercase and strip whitespace
                    normalized_email = email_raw.lower().strip()
                    
                    # Verify minimum requirements
                    if len(normalized_email) >= 6 and normalized_email.count('@') == 1:
                        # Hash the normalized email
                        hashed_email = hashlib.sha256(normalized_email.encode('utf-8')).hexdigest()
                        self.email_processed_count += 1
                except Exception as e:
                    self.logger.warning(f"Error processing email for customer {cust_no}: {e}")
            
            # Process phone number
            if phone_raw and len(phone_raw) >= 10:
                try:
                    import phonenumbers
                    
                    # Clean the phone number - remove non-numeric chars
                    clean_phone = ''.join(c for c in phone_raw if c.isdigit())
                    
                    # Try to parse the phone with US as default region
                    if clean_phone:
                        try:
                            phone_obj = phonenumbers.parse(clean_phone, self.default_region)
                        except:
                            # Try again with a '+' prefix if it might be an international format
                            if clean_phone.startswith('1') and len(clean_phone) > 10:
                                try:
                                    phone_obj = phonenumbers.parse('+' + clean_phone)
                                except:
                                    phone_obj = None
                            else:
                                phone_obj = None
                        
                        if phone_obj and phonenumbers.is_valid_number(phone_obj):
                            # Format to E.164 standard format
                            formatted_phone = phonenumbers.format_number(
                                phone_obj, phonenumbers.PhoneNumberFormat.E164)
                            
                            # Generate the SHA-256 hash
                            hashed_phone = hashlib.sha256(formatted_phone.encode('utf-8')).hexdigest()
                            self.phone_processed_count += 1
                except Exception as e:
                    self.logger.warning(f"Error processing phone for customer {cust_no}: {e}")
            
            # Process name and address for improved matching
            has_valid_name = False
            if first_name_raw and last_name_raw:
                # Clean and normalize names
                first_name = first_name_raw.lower().strip()
                last_name = last_name_raw.lower().strip()
                
                # Check for problematic characters
                has_problem_chars = any(char in first_name + last_name for char in problem_chars)
                
                if not has_problem_chars and len(first_name) >= 2 and len(last_name) >= 2:
                    # Hash names
                    hashed_first_name = hashlib.sha256(first_name.encode('utf-8')).hexdigest()
                    hashed_last_name = hashlib.sha256(last_name.encode('utf-8')).hexdigest()
                    has_valid_name = True
                else:
                    skip_name_for_address = True
            
            # Process US address components (zip code and state)
            if zip_code_raw and len(zip_code_raw) >= 5:
                # For US addresses
                country_code_to_use = "US"
                
                # Clean zip code - take first 5 digits
                clean_zip = ''.join(c for c in zip_code_raw if c.isdigit())[:5]
                
                if len(clean_zip) == 5:
                    postal_code_to_use = clean_zip
                    
                    # Only count as address match if we have name or will process name
                    if has_valid_name and not skip_name_for_address:
                        self.address_processed_count += 1
            
            # Create user identifiers for Google Ads
            user_identifiers = []
            
            # Add email identifier if available
            if hashed_email:
                user_identifiers.append({
                    "hashed_email": hashed_email
                })
            
            # Add phone identifier if available
            if hashed_phone:
                user_identifiers.append({
                    "hashed_phone_number": hashed_phone
                })
            
            # Add address identifier if all components are available
            if country_code_to_use and postal_code_to_use and hashed_first_name and hashed_last_name and not skip_name_for_address:
                user_identifiers.append({
                    "address_info": {
                        "hashed_first_name": hashed_first_name,
                        "hashed_last_name": hashed_last_name,
                        "country_code": country_code_to_use,
                        "postal_code": postal_code_to_use
                    }
                })
            
            # Only process customers with at least one identifier
            if user_identifiers:
                self.rows_with_any_id_count += 1
                
                # Track brand for reporting purposes
                brand_info = brand_raw if brand_raw else "unspecified"
                
                # Store the operation for batch upload
                self.processed_operations.append({
                    "user_identifiers": user_identifiers,
                    "cust_no": cust_no,
                    "contact_guid": str(contact_guid),
                    "brand": brand_info
                })
                
        except Exception as e:
            self.logger.warning(f"Error processing customer row: {e}")
    
    def upload_to_google_ads(self):
        """Upload the processed customer data to Google Ads."""
        if not self.googleads_client:
            self.logger.error("Google Ads client not initialized.")
            return False
        
        if not self.processed_operations:
            self.logger.warning("No operations to upload.")
            return True
        
        self.logger.info(f"Starting upload to Google Ads User List {self.user_list_id}...")
        
        # Generate a tracking ID for this upload operation
        self.tracking_id = f"upload_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{random.randint(10000, 99999)}"
        
        # Get the UserListService
        try:
            offline_user_data_job_service = self.googleads_client.get_service("OfflineUserDataJobService")
            user_list_service = self.googleads_client.get_service("UserListService")
        except Exception as e:
            self.logger.error(f"Failed to get Google Ads services: {e}")
            return False
        
        # Get customer ID from config
        customer_id = self.config["GADS_CUSTOMER_ID"]
        
        try:
            # Get user list details for logging
            user_list_resource_name = user_list_service.user_list_path(customer_id, self.user_list_id)
            try:
                user_list = user_list_service.get_user_list(resource_name=user_list_resource_name)
                self.logger.info(f"Uploading to list: '{user_list.name}' (ID: {self.user_list_id})")
                if self.brand and self.brand != "default":
                    self.logger.info(f"Brand-specific upload for: {self.brand}")
            except Exception as e:
                self.logger.warning(f"Could not retrieve user list details: {e}")
                self.logger.info(f"Proceeding with upload to user list ID: {self.user_list_id}")
            
            # Create an offline user data job for adding users to the customer match list
            offline_user_data_job = {
                "type_": "CUSTOMER_MATCH_USER_LIST",
                "customer_match_user_list_metadata": {
                    "user_list": user_list_resource_name,
                }
            }
            
            create_offline_user_data_job_response = (
                offline_user_data_job_service.create_offline_user_data_job(
                    customer_id=customer_id, job=offline_user_data_job
                )
            )
            
            offline_user_data_job_resource_name = create_offline_user_data_job_response.resource_name
            self.logger.info(f"Created offline user data job: {offline_user_data_job_resource_name}")
            
            # Process operations in batches
            operations_count = len(self.processed_operations)
            batch_size = self.config["API_BATCH_SIZE"]
            batches_count = (operations_count + batch_size - 1) // batch_size
            
            self.logger.info(f"Uploading {operations_count} operations in {batches_count} batches...")
            
            for batch_num in range(batches_count):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, operations_count)
                current_batch = self.processed_operations[start_idx:end_idx]
                
                # Create operations for this batch
                operations = []
                for customer_data in current_batch:
                    operation = {
                        "create": {
                            "user_identifiers": [
                                self._convert_to_user_identifier(identifier)
                                for identifier in customer_data["user_identifiers"]
                            ]
                        }
                    }
                    operations.append(operation)
                
                # Try to add the operations with retries
                success = False
                for attempt in range(self.config["API_RETRY_COUNT"]):
                    try:
                        # Add operations
                        request = {
                            "resource_name": offline_user_data_job_resource_name,
                            "operations": operations,
                        }
                        
                        response = offline_user_data_job_service.add_operations(request=request)
                        
                        self.logger.info(
                            f"Batch {batch_num + 1}/{batches_count}: "
                            f"Added {len(current_batch)} operations successfully."
                        )
                        success = True
                        
                        # Add delay to prevent hitting API rate limits
                        time.sleep(self.config["API_RATE_LIMIT_DELAY"])
                        break
                        
                    except Exception as e:
                        error_message = str(e)
                        if "ConcurrentModificationException" in error_message:
                            retry_delay = self.config["API_RETRY_DELAY_BASE"] ** attempt
                            self.logger.warning(
                                f"Concurrent modification detected on batch {batch_num + 1}. "
                                f"Retrying in {retry_delay:.1f} seconds... (Attempt {attempt + 1}/{self.config['API_RETRY_COUNT']})"
                            )
                            time.sleep(retry_delay)
                        else:
                            self.logger.error(f"Error adding operations in batch {batch_num + 1}: {e}")
                            break
                
                if not success:
                    self.logger.error(f"Failed to add batch {batch_num + 1} after {self.config['API_RETRY_COUNT']} attempts.")
            
            # Run the job
            offline_user_data_job_service.run_offline_user_data_job(
                resource_name=offline_user_data_job_resource_name
            )
            self.logger.info(f"Started offline user data job: {offline_user_data_job_resource_name}")
            
            return True
            
        except Exception as e:
            self.logger.exception(f"Error in upload_to_google_ads: {e}")
            return False
    
    def _convert_to_user_identifier(self, identifier):
        """Convert a processed user identifier to the Google Ads API format."""
        if "hashed_email" in identifier:
            return {
                "hashed_email": identifier["hashed_email"]
            }
        elif "hashed_phone_number" in identifier:
            return {
                "hashed_phone_number": identifier["hashed_phone_number"]
            }
        elif "address_info" in identifier:
            address = identifier["address_info"]
            return {
                "address_info": {
                    "hashed_first_name": address["hashed_first_name"],
                    "hashed_last_name": address["hashed_last_name"],
                    "country_code": address["country_code"],
                    "postal_code": address["postal_code"]
                }
            }
        else:
            raise ValueError("Unknown identifier type")
    
    def wait_for_job_completion(self, job_resource_name, timeout_seconds=300):
        """Wait for a Google Ads offline user data job to complete."""
        if not self.googleads_client:
            self.logger.error("Google Ads client not initialized.")
            return False
        
        self.logger.info(f"Monitoring job completion: {job_resource_name}")
        
        try:
            offline_user_data_job_service = self.googleads_client.get_service("OfflineUserDataJobService")
            
            start_time = time.time()
            while time.time() - start_time < timeout_seconds:
                job = offline_user_data_job_service.get_offline_user_data_job(
                    resource_name=job_resource_name
                )
                
                status = job.status
                self.logger.info(f"Current job status: {status}")
                
                if status == 5:  # SUCCESS
                    self.logger.info(f"Job completed successfully.")
                    return True
                elif status in [4, 6]:  # FAILED, CANCELED
                    self.logger.error(f"Job failed with status: {status}")
                    return False
                
                # Status 1: PENDING, 2: RUNNING
                # Sleep for 10 seconds before checking again
                time.sleep(10)
            
            self.logger.warning(f"Job monitoring timed out after {timeout_seconds} seconds.")
            return False
            
        except Exception as e:
            self.logger.exception(f"Error monitoring job: {e}")
            return False
    
    def create_brand_specific_user_list(self, brand_name, description=None):
        """Create a new user list for a specific brand."""
        if not self.googleads_client:
            self.logger.error("Google Ads client not initialized.")
            return None
        
        if not brand_name:
            self.logger.error("Brand name is required to create a user list.")
            return None
        
        description = description or f"Customer list for {brand_name} brand"
        list_name = f"{brand_name.title()} Brand Customers - {datetime.now().strftime('%Y-%m-%d')}"
        
        try:
            # Get services
            user_list_service = self.googleads_client.get_service("UserListService")
            customer_id = self.config["GADS_CUSTOMER_ID"]
            
            # Create user list
            user_list_operation = self.googleads_client.get_type("UserListOperation")
            user_list = user_list_operation.create
            user_list.name = list_name
            user_list.description = description
            user_list.crm_based_user_list.upload_key_type = (
                self.googleads_client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
            )
            user_list.crm_based_user_list.app_id = ""
            
            # Add membership life span (default to 540 days)
            user_list.membership_life_span = 540
            
            # Create the user list
            response = user_list_service.mutate_user_lists(
                customer_id=customer_id, operations=[user_list_operation]
            )
            
            user_list_id = response.results[0].resource_name.split('/')[-1]
            self.logger.info(f"Created new user list for brand '{brand_name}': {list_name} (ID: {user_list_id})")
            
            # Add to brands configuration
            if "BRANDS" not in self.config:
                self.config["BRANDS"] = {}
            
            self.config["BRANDS"][brand_name.lower()] = {
                "USER_LIST_ID": user_list_id,
                "DESCRIPTION": description
            }
            
            # Update available brands
            self.available_brands = list(self.config["BRANDS"].keys())
            
            return user_list_id
            
        except Exception as e:
            self.logger.exception(f"Error creating user list for brand '{brand_name}': {e}")
            return None
    
    def list_available_user_lists(self):
        """List all available Customer Match user lists."""
        if not self.googleads_client:
            self.logger.error("Google Ads client not initialized.")
            return []
        
        try:
            # Get services
            google_ads_service = self.googleads_client.get_service("GoogleAdsService")
            customer_id = self.config["GADS_CUSTOMER_ID"]
            
            # Create query for user lists
            query = """
                SELECT
                  user_list.id,
                  user_list.name,
                  user_list.description,
                  user_list.membership_life_span,
                  user_list.size_for_display,
                  user_list.size_for_search,
                  user_list.type,
                  user_list.read_only
                FROM user_list
                WHERE user_list.type = 'CRM_BASED'
                ORDER BY user_list.id
            """
            
            # Issue a search request
            response = google_ads_service.search(customer_id=customer_id, query=query)
            
            # Process and return the results
            user_lists = []
            for row in response:
                user_list = row.user_list
                user_lists.append({
                    "id": user_list.id,
                    "name": user_list.name,
                    "description": user_list.description,
                    "membership_life_span": user_list.membership_life_span,
                    "size_for_display": user_list.size_for_display,
                    "size_for_search": user_list.size_for_search,
                    "type": user_list.type.name,
                    "read_only": user_list.read_only
                })
            
            self.logger.info(f"Found {len(user_lists)} Customer Match user lists.")
            return user_lists
            
        except Exception as e:
            self.logger.exception(f"Error listing user lists: {e}")
            return []
    
    def run_upload_process(self):
        """Run the complete customer data upload process."""
        # Initialize connections
        if not self.initialize_db_connection():
            return False
        
        if not self.initialize_google_ads_client():
            return False
        
        # Fetch and process customer data
        if not self.fetch_and_process_customer_data():
            return False
        
        # Upload to Google Ads
        if not self.upload_to_google_ads():
            return False
        
        # Display summary
        self.logger.info("\nProcess Completed Successfully.")
        self.logger.info(f"Total rows processed: {self.total_rows_processed}")
        self.logger.info(f"Rows with any identifier: {self.rows_with_any_id_count}")
        self.logger.info(f"Email identifiers processed: {self.email_processed_count}")
        self.logger.info(f"Phone identifiers processed: {self.phone_processed_count}")
        self.logger.info(f"Address identifiers processed: {self.address_processed_count}")
        
        if self.brand:
            self.logger.info(f"Brand filter applied: {self.brand}")
        
        return True


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Google Ads Customer Data Upload Tool")
    
    # Main operation modes
    parser.add_argument(
        "--mode", 
        choices=["delta", "full"],
        default="delta",
        help="Upload mode: 'delta' for incremental updates, 'full' for complete replacement (default: delta)"
    )
    
    # Brand-specific options
    parser.add_argument(
        "--brand",
        help="Upload data for a specific brand only"
    )
    
    parser.add_argument(
        "--create-brand-list",
        metavar="BRAND_NAME", 
        help="Create a new user list for the specified brand"
    )
    
    parser.add_argument(
        "--list-brands",
        action="store_true",
        help="List all available brand configurations"
    )
    
    parser.add_argument(
        "--list-user-lists",
        action="store_true",
        help="List all available customer match user lists"
    )
    
    # Configuration
    parser.add_argument(
        "--config", 
        help="Path to configuration file (YAML or JSON)"
    )
    
    parser.add_argument(
        "--debug-venv", 
        action="store_true",
        help="Output virtual environment diagnostic information"
    )
    
    return parser.parse_args()


def main():
    """Main function to run the upload tool."""
    # Ensure script runs in a virtual environment
    ensure_venv()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config(args.config)
    
    # Setup logging
    logger = setup_logging(config["LOG_FILE_PATH"])
    logger.info(f"Google Ads Customer Data Upload Tool - Starting")
    logger.info(f"Run mode: {args.mode}")
    
    if args.brand:
        logger.info(f"Brand filter: {args.brand}")
    
    # Handle special command modes
    if args.list_brands:
        logger.info("Listing available brand configurations:")
        for brand, brand_config in config.get("BRANDS", {}).items():
            logger.info(f"  - {brand}: User List ID {brand_config.get('USER_LIST_ID')}")
        return
    
    if args.create_brand_list:
        uploader = GoogleAdsUploader(config)
        if uploader.initialize_google_ads_client():
            brand_description = f"Customer list for {args.create_brand_list} brand"
            user_list_id = uploader.create_brand_specific_user_list(
                args.create_brand_list, 
                description=brand_description
            )
            if user_list_id:
                logger.info(f"Successfully created user list for brand '{args.create_brand_list}' with ID: {user_list_id}")
            else:
                logger.error(f"Failed to create user list for brand '{args.create_brand_list}'")
        return
    
    if args.list_user_lists:
        uploader = GoogleAdsUploader(config)
        if uploader.initialize_google_ads_client():
            user_lists = uploader.list_available_user_lists()
            logger.info("Available Customer Match user lists:")
            for user_list in user_lists:
                logger.info(f"  - {user_list['name']} (ID: {user_list['id']})")
                logger.info(f"    Description: {user_list['description']}")
                logger.info(f"    Size: {user_list['size_for_display']} (display), {user_list['size_for_search']} (search)")
                logger.info(f"    Membership life span: {user_list['membership_life_span']} days")
                logger.info(f"    Read-only: {user_list['read_only']}")
                logger.info("")
        return
    
    # Initialize uploader and run process
    uploader = GoogleAdsUploader(config, run_mode=args.mode, brand=args.brand)
    result = uploader.run_upload_process()
    
    if result:
        logger.info("Process completed successfully.")
    else:
        logger.error("Process failed. Check logs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
