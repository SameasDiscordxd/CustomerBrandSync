import argparse
import csv
import hashlib
import os
import sys
from typing import List, Dict
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Fix Unicode encoding for Windows
if os.name == 'nt':  # Windows
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

class GoogleAdsUploader:
    def __init__(self, customer_id: str):
        """Initialize the Google Ads client"""
        self.customer_id = customer_id
        
        # Initialize the Google Ads client from environment variables
        self.client = GoogleAdsClient.load_from_env(version="v17")  # Specify API version explicitly
        
    def hash_data(self, data: str) -> str:
        """Hash sensitive data using SHA-256"""
        if not data:
            return ""
        return hashlib.sha256(data.strip().lower().encode('utf-8')).hexdigest()
    
    def create_user_data_operations(self, customers: List[Dict]) -> List:
        """Create user data operations for Google Ads upload"""
        operations = []
        
        for customer in customers:
            # Create a new UserDataOperation for each customer
            operation = self.client.get_type("UserDataOperation")
            user_data = operation.create
            
            # Create user identifiers list
            user_identifiers = []
            
            # Add hashed email if available
            if customer.get('email'):
                email_identifier = self.client.get_type("UserIdentifier")
                email_identifier.hashed_email = self.hash_data(customer['email'])
                user_identifiers.append(email_identifier)
            
            # Add hashed phone if available
            if customer.get('phone'):
                phone_identifier = self.client.get_type("UserIdentifier")
                phone_identifier.hashed_phone_number = self.hash_data(customer['phone'])
                user_identifiers.append(phone_identifier)
            
            # Add address information if available
            if any(customer.get(field) for field in ['first_name', 'last_name', 'country', 'zip']):
                address_identifier = self.client.get_type("UserIdentifier")
                address_info = address_identifier.address_info
                
                if customer.get('first_name'):
                    address_info.hashed_first_name = self.hash_data(customer['first_name'])
                if customer.get('last_name'):
                    address_info.hashed_last_name = self.hash_data(customer['last_name'])
                if customer.get('country'):
                    address_info.country_code = customer['country'].upper()
                if customer.get('zip'):
                    address_info.postal_code = customer['zip']
                
                user_identifiers.append(address_identifier)
            
            # Add user identifiers to user data
            user_data.user_identifiers.extend(user_identifiers)
            operations.append(operation)
        
        return operations
    
    def upload_to_audience_list(self, list_id: str, operations: List, segment_name: str) -> bool:
        """Upload user data operations to a specific audience list"""
        try:
            # Get the OfflineUserDataJobService
            offline_user_data_job_service = self.client.get_service("OfflineUserDataJobService")
            
            # Create offline user data job
            offline_user_data_job = self.client.get_type("OfflineUserDataJob")
            offline_user_data_job.type_ = self.client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
            offline_user_data_job.customer_match_user_list_metadata.user_list = f"customers/{self.customer_id}/userLists/{list_id}"
            
            # Create the job
            create_job_response = offline_user_data_job_service.create_offline_user_data_job(
                customer_id=self.customer_id,
                job=offline_user_data_job
            )
            
            job_resource_name = create_job_response.resource_name
            print(f"[SUCCESS] Created offline user data job: {job_resource_name}")
            
            # Upload operations in batches of 10,000
            batch_size = 10000
            total_operations = len(operations)
            
            for i in range(0, total_operations, batch_size):
                batch = operations[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_operations + batch_size - 1) // batch_size
                
                print(f"[UPLOAD] Uploading batch {batch_num}/{total_batches} ({len(batch)} operations)")
                
                # Add operations to the job
                request = self.client.get_type("AddOfflineUserDataJobOperationsRequest")
                request.resource_name = job_resource_name
                request.operations = batch
                request.enable_partial_failure = True
                
                response = offline_user_data_job_service.add_offline_user_data_job_operations(request=request)
                
                if response.partial_failure_error:
                    print(f"[WARNING] Batch {batch_num} had partial failures: {response.partial_failure_error}")
                else:
                    print(f"[SUCCESS] Batch {batch_num} uploaded successfully")
            
            # Run the job
            print("[RUNNING] Running the offline user data job...")
            offline_user_data_job_service.run_offline_user_data_job(
                resource_name=job_resource_name
            )
            
            print(f"[SUCCESS] Successfully initiated upload of {total_operations} operations to {segment_name} (List ID: {list_id})")
            return True
            
        except GoogleAdsException as ex:
            print(f"[ERROR] Google Ads API error occurred:")
            for error in ex.failure.errors:
                print(f"   Error: {error.message}")
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"   Field: {field_path_element.field_name}")
            return False
        except Exception as ex:
            print(f"[ERROR] Unexpected error: {str(ex)}")
            return False

def read_csv_customers(csv_file: str) -> List[Dict]:
    """Read customer data from CSV file"""
    customers = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                customers.append(row)
        print(f"[SUCCESS] Read {len(customers)} customers from CSV file")
        return customers
    except Exception as ex:
        print(f"[ERROR] Error reading CSV file: {str(ex)}")
        return []

def main():
    """Main function to handle command line arguments and execute upload"""
    parser = argparse.ArgumentParser(description='Upload customer data to Google Ads audience lists')
    parser.add_argument('--customer-id', required=True, help='Google Ads customer ID')
    parser.add_argument('--list-id', required=True, help='Google Ads user list ID')
    parser.add_argument('--csv-file', required=True, help='Path to CSV file with customer data')
    parser.add_argument('--segment-name', required=True, help='Segment name for logging')
    
    args = parser.parse_args()
    
    print("Google Ads Customer Upload Tool")
    print(f"   Customer ID: {args.customer_id}")
    print(f"   List ID: {args.list_id}")
    print(f"   CSV File: {args.csv_file}")
    print(f"   Segment: {args.segment_name}")
    print()
    
    try:
        # Initialize uploader
        uploader = GoogleAdsUploader(args.customer_id)
        print("[SUCCESS] Google Ads client initialized successfully")
        
        # Read customer data
        customers = read_csv_customers(args.csv_file)
        if not customers:
            print("[ERROR] No customer data found")
            return
        
        # Create user data operations
        operations = uploader.create_user_data_operations(customers)
        print(f"[SUCCESS] Created {len(operations)} user data operations")
        
        # Upload to audience list
        success = uploader.upload_to_audience_list(args.list_id, operations, args.segment_name)
        
        if success:
            print(f"[COMPLETE] Upload completed successfully for {args.segment_name}")
        else:
            print(f"[ERROR] Upload failed for {args.segment_name}")
            exit(1)
            
    except Exception as ex:
        print(f"[FATAL] Fatal error: {str(ex)}")
        exit(1)

if __name__ == "__main__":
    main()