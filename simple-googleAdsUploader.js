#!/usr/bin/env node
/**
 * Google Ads Customer Data Upload Tool
 * 
 * This script automates the upload of customer data to Google Ads for Customer Match audiences,
 * supporting both full and incremental (delta) uploads with brand-specific targeting.
 * 
 * Features:
 * - Automatic dependency management
 * - Retry logic for handling concurrent modification errors
 * - Rate limiting to prevent API rate limits
 * - Brand-specific filtering capability
 */

// Core dependencies
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Third-party dependencies
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
const winston = require('winston');
const dotenv = require('dotenv');
const sql = require('mssql');
const { parsePhoneNumber } = require('libphonenumber-js');
const { GoogleAdsApi } = require('google-ads-api');

// Load environment variables
dotenv.config();

// Initialize logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp }) => {
      return `${timestamp} - ${level.toUpperCase()}: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'googleAdsUploader.log' })
  ]
});

/**
 * Default configuration
 * These can be overridden by .env file or command line arguments
 */
const DEFAULT_CONFIG = {
  DB_SERVER: process.env.DB_SERVER || "bbtdatanew.privatelink.westus.cloudapp.azure.com,1401",
  DB_DATABASE: process.env.DB_DATABASE || "Venom",
  DB_USERNAME: process.env.DB_USERNAME || "APIUser",
  DB_PASSWORD: process.env.DB_PASSWORD,
  GADS_CUSTOMER_ID: process.env.GADS_CUSTOMER_ID || "4018470779",
  USER_LIST_ID: process.env.DEFAULT_USER_LIST_ID || "9027934773",
  API_BATCH_SIZE: parseInt(process.env.API_BATCH_SIZE) || 2500,  
  API_RETRY_COUNT: parseInt(process.env.API_RETRY_COUNT) || 3,
  API_RETRY_DELAY_BASE: parseInt(process.env.API_RETRY_DELAY_BASE) || 2,
  API_RATE_LIMIT_DELAY: parseFloat(process.env.API_RATE_LIMIT_DELAY) || 0.5
};

/**
 * Command line argument configuration
 */
const argv = yargs(hideBin(process.argv))
  .option('mode', {
    alias: 'm',
    description: 'Upload mode: "delta" for incremental updates, "full" for complete replacement',
    type: 'string',
    choices: ['delta', 'full'],
    default: 'delta'
  })
  .option('brand', {
    alias: 'b',
    description: 'Filter data for a specific brand only',
    type: 'string'
  })
  .option('stats-only', {
    description: 'Only gather and display statistics without uploading to Google Ads',
    type: 'boolean',
    default: false
  })
  .option('silent', {
    description: 'Reduce console output (for automated runs). Still logs to file.',
    type: 'boolean',
    default: false
  })
  .option('config', {
    alias: 'c',
    description: 'Path to configuration file (JSON)',
    type: 'string'
  })
  .help()
  .alias('help', 'h')
  .argv;

/**
 * Google Ads Uploader class
 */
class GoogleAdsUploader {
  constructor(config, runMode = 'delta', brand = null) {
    this.config = config;
    this.runMode = runMode;
    this.brand = brand;
    this.runId = crypto.randomUUID();
    logger.info(`Generated unique run ID: ${this.runId}`);
    
    // Initialize counters
    this.totalRowsProcessed = 0;
    this.emailProcessedCount = 0;
    this.phoneProcessedCount = 0;
    this.addressProcessedCount = 0;
    this.rowsWithAnyIdCount = 0;
    this.processedOperations = [];
    
    // Default values
    this.defaultRegion = "US";
    this.fetchBatchSize = 10000;
    this.trackingId = null;
    
    // Initialize clients
    this.dbConn = null;
    this.googleAdsClient = null;
    
    // Single user list for all brands
    this.userListId = config.USER_LIST_ID;
    logger.info(`Using User List ID: ${this.userListId}`);
  }

  /**
   * Initialize the database connection
   */
  async initializeDbConnection() {
    // Check if password is provided
    if (!this.config.DB_PASSWORD) {
      logger.error("FATAL: DB_PASSWORD not set in configuration or environment variables.");
      throw new Error("Missing database password");
    }
    
    // Build connection configuration
    const sqlConfig = {
      user: this.config.DB_USERNAME,
      password: this.config.DB_PASSWORD,
      database: this.config.DB_DATABASE,
      server: this.config.DB_SERVER.split(',')[0],
      port: parseInt(this.config.DB_SERVER.split(',')[1] || '1433'),
      pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
      },
      options: {
        encrypt: true,
        trustServerCertificate: true,
        connectionTimeout: 30000,
        requestTimeout: 30000
      }
    };
    
    try {
      this.dbConn = await sql.connect(sqlConfig);
      logger.info(`Connected to database: ${this.config.DB_DATABASE} on ${this.config.DB_SERVER}`);
      return true;
    } catch (err) {
      logger.error(`FATAL: Database connection failed: ${err.message}`);
      throw err;
    }
  }

  /**
   * Initialize the Google Ads API client
   */
  initializeGoogleAdsClient() {
    try {
      // Authentication is typically done through environment variables:
      // GOOGLE_ADS_REFRESH_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_DEVELOPER_TOKEN
      this.googleAdsClient = new GoogleAdsApi({
        client_id: process.env.GOOGLE_ADS_CLIENT_ID,
        client_secret: process.env.GOOGLE_ADS_CLIENT_SECRET,
        developer_token: process.env.GOOGLE_ADS_DEVELOPER_TOKEN
      });
      
      // Check if we have a valid refresh token
      if (!process.env.GOOGLE_ADS_REFRESH_TOKEN) {
        logger.error("FATAL: Missing GOOGLE_ADS_REFRESH_TOKEN environment variable");
        throw new Error("Missing Google Ads refresh token");
      }
      
      logger.info("Google Ads Client Initialized Successfully.");
      return true;
    } catch (err) {
      logger.error(`FATAL: Failed to initialize Google Ads Client: ${err.message}`);
      throw err;
    }
  }

  /**
   * Fetch customer data from database and process for Google Ads upload
   */
  async fetchAndProcessCustomerData() {
    if (!this.dbConn) {
      logger.error("Database connection not initialized.");
      return false;
    }
    
    // Use the existing stored procedure
    let fullUploadValue = this.runMode === "full" ? 1 : 0;
    let request = this.dbConn.request();
    
    try {
      // Add parameters
      request.input('FullUpload', sql.Bit, fullUploadValue);
      
      // Add brand parameter if specified
      if (this.brand) {
        request.input('Brand', sql.NVarChar, this.brand);
        logger.info(`Executing stored procedure with @FullUpload = ${fullUploadValue}, @Brand = '${this.brand}'`);
      } else {
        logger.info(`Executing stored procedure with @FullUpload = ${fullUploadValue}`);
      }
      
      // Execute the stored procedure - Using the existing one that handles brand filtering
      const result = await request.execute('dbo.GetNewCustomersForGoogleAds');
      logger.info("Stored procedure executed. Processing results...");
      
      // Process the data
      if (result.recordset && result.recordset.length > 0) {
        for (const row of result.recordset) {
          this.totalRowsProcessed++;
          await this._processCustomerRow(row);
        }
      }
      
      logger.info(`Finished fetching and processing.`);
      logger.info(`  Total rows processed from database: ${this.totalRowsProcessed}`);
      logger.info(`  Operations prepared for Google Ads: ${this.processedOperations.length}`);
      return true;
    } catch (err) {
      logger.error(`FATAL ERROR during database processing: ${err.message}`);
      return false;
    }
  }

  /**
   * Process a single customer data row
   */
  async _processCustomerRow(row) {
    try {
      // Extract and clean customer data fields
      const custNo = row.CustomerNo ? row.CustomerNo.trim() : null;
      const firstNameRaw = row.FirstName ? row.FirstName.trim() : null;
      const lastNameRaw = row.LastName ? row.LastName.trim() : null;
      const contactGuid = row.ContactGUID;
      const emailRaw = row.Email ? row.Email.trim() : null;
      const phoneRaw = row.Phone ? row.Phone.trim() : null;
      const zipCodeRaw = row.ZipCode ? row.ZipCode.trim() : null;
      const stateCodeRaw = row.StateCode ? row.StateCode.trim() : null;
      
      // Check for brand column (added for brand-specific targeting)
      let brandRaw = null;
      if (row.Brand) {
        brandRaw = row.Brand.trim().toLowerCase();
      }
      
      // Skip if we're filtering by brand and this row doesn't match
      if (this.brand && brandRaw && brandRaw.toLowerCase() !== this.brand.toLowerCase()) {
        return;
      }
      
      // Initialize variables for hashed identifiers
      let hashedEmail = null;
      let hashedPhone = null;
      let hashedFirstName = null;
      let hashedLastName = null;
      let countryCodeToUse = null;
      let postalCodeToUse = null;
      let skipNameForAddress = false;
      const problemChars = ['/', '&', '"', ';', ':', '#', '*'];
      
      // Process email
      if (emailRaw && emailRaw.includes('@')) {
        const normalizedEmail = emailRaw.toLowerCase();
        hashedEmail = this._normalizeAndHashString(normalizedEmail);
        this.emailProcessedCount++;
      }
      
      // Process phone
      if (phoneRaw && phoneRaw.length >= 10) {
        try {
          // Attempt to parse with libphonenumber-js
          const phoneNumber = parsePhoneNumber(phoneRaw, this.defaultRegion);
          if (phoneNumber && phoneNumber.isValid()) {
            // Extract the E.164 format (no spaces, with country code)
            const e164Format = phoneNumber.number;
            hashedPhone = this._normalizeAndHashString(e164Format);
            this.phoneProcessedCount++;
          }
        } catch (phoneErr) {
          // If parsing fails, try simple normalization for US numbers
          const digitsOnly = phoneRaw.replace(/\D/g, '');
          if (digitsOnly.length >= 10) {
            // If the number doesn't start with country code, add US +1
            const e164Format = digitsOnly.length === 10 ? `+1${digitsOnly}` : `+${digitsOnly}`;
            hashedPhone = this._normalizeAndHashString(e164Format);
            this.phoneProcessedCount++;
          }
        }
      }
      
      // Process name for address-based matching
      if (zipCodeRaw && zipCodeRaw.length >= 5) {
        // Clean and prepare the postal code
        postalCodeToUse = zipCodeRaw.substring(0, 5);
        countryCodeToUse = "US"; // Default to US
        
        // Check if first and last name can be used (no problematic characters)
        let hasProblematicChars = false;
        if (firstNameRaw && lastNameRaw) {
          for (const char of problemChars) {
            if (firstNameRaw.includes(char) || lastNameRaw.includes(char)) {
              hasProblematicChars = true;
              break;
            }
          }
          
          if (!hasProblematicChars) {
            hashedFirstName = this._normalizeAndHashString(firstNameRaw.toLowerCase());
            hashedLastName = this._normalizeAndHashString(lastNameRaw.toLowerCase());
          } else {
            skipNameForAddress = true;
          }
        } else {
          skipNameForAddress = true;
        }
        
        this.addressProcessedCount++;
      }
      
      // If we have at least one valid identifier, create and add the operation
      if (hashedEmail || hashedPhone || (postalCodeToUse && countryCodeToUse)) {
        this.rowsWithAnyIdCount++;
        
        const operation = this._createGoogleAdsOperation(
          hashedEmail,
          hashedPhone,
          hashedFirstName,
          hashedLastName,
          countryCodeToUse,
          postalCodeToUse,
          skipNameForAddress
        );
        
        if (operation) {
          this.processedOperations.push(operation);
        }
      }
      
    } catch (err) {
      logger.warn(`Error processing row: ${err.message}`);
    }
  }

  /**
   * Normalize and hash string for Google Ads
   */
  _normalizeAndHashString(input) {
    if (!input) return null;
    
    try {
      // Normalize and hash the string using SHA-256
      const hash = crypto.createHash('sha256');
      hash.update(input);
      return hash.digest('hex');
    } catch (err) {
      logger.warn(`Error hashing string: ${err.message}`);
      return null;
    }
  }

  /**
   * Create a Google Ads operation from processed customer data
   */
  _createGoogleAdsOperation(
    hashedEmail,
    hashedPhone,
    hashedFirstName,
    hashedLastName,
    countryCode,
    postalCode,
    skipNameForAddress
  ) {
    // Initialize the operation object
    const operation = {
      userIdentifiers: []
    };
    
    // Add email identifier if available
    if (hashedEmail) {
      operation.userIdentifiers.push({
        hashedEmail: hashedEmail
      });
    }
    
    // Add phone identifier if available
    if (hashedPhone) {
      operation.userIdentifiers.push({
        hashedPhoneNumber: hashedPhone
      });
    }
    
    // Add address identifier if available
    if (postalCode && countryCode) {
      const addressInfo = {
        hashedPostalCode: this._normalizeAndHashString(postalCode),
        countryCode: countryCode
      };
      
      // Add name components if available and not skipped
      if (!skipNameForAddress && hashedFirstName && hashedLastName) {
        addressInfo.hashedFirstName = hashedFirstName;
        addressInfo.hashedLastName = hashedLastName;
      }
      
      operation.userIdentifiers.push({
        addressInfo: addressInfo
      });
    }
    
    // Only return the operation if it has at least one identifier
    return operation.userIdentifiers.length > 0 ? operation : null;
  }

  /**
   * Upload processed operations to Google Ads
   */
  async uploadToGoogleAds() {
    if (!this.googleAdsClient) {
      logger.error("Google Ads client not initialized.");
      return false;
    }
    
    if (this.processedOperations.length === 0) {
      logger.warn("No operations to upload.");
      return true; // Not an error, just nothing to do
    }
    
    try {
      logger.info(`Starting upload to Google Ads. Operations to upload: ${this.processedOperations.length}`);
      
      // Create a Customer object
      const customer = this.googleAdsClient.Customer({
        customer_id: this.config.GADS_CUSTOMER_ID,
        refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN
      });
      
      // Get the appropriate service
      const offlineUserDataJobService = customer.getService('OfflineUserDataJobService');
      
      // Create an offline user data job
      const createJobResponse = await offlineUserDataJobService.create({
        customerId: this.config.GADS_CUSTOMER_ID,
        job: {
          type: 'CUSTOMER_MATCH_USER_LIST',
          customerMatchUserListMetadata: {
            userList: `customers/${this.config.GADS_CUSTOMER_ID}/userLists/${this.userListId}`
          }
        }
      });
      
      const jobResourceName = createJobResponse.resourceName;
      logger.info(`Created offline user data job: ${jobResourceName}`);
      
      // Upload operations in batches
      const batchSize = this.config.API_BATCH_SIZE;
      const batches = [];
      
      for (let i = 0; i < this.processedOperations.length; i += batchSize) {
        batches.push(this.processedOperations.slice(i, i + batchSize));
      }
      
      logger.info(`Split operations into ${batches.length} batches of up to ${batchSize} operations each`);
      
      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        logger.info(`Processing batch ${i + 1} of ${batches.length} (${batch.length} operations)`);
        
        // Use retry logic for the batch upload
        await this._uploadBatchWithRetry(
          offlineUserDataJobService,
          jobResourceName,
          batch,
          i + 1,
          batches.length
        );
        
        // Add delay between batches to avoid rate limiting
        if (i < batches.length - 1) {
          const delayMs = this.config.API_RATE_LIMIT_DELAY * 1000;
          logger.info(`Waiting ${delayMs}ms before next batch to avoid rate limiting...`);
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
      }
      
      // Run the job
      await offlineUserDataJobService.run({
        resourceName: jobResourceName
      });
      
      logger.info(`Job started. Waiting for completion...`);
      
      // Wait for the job to complete
      const success = await this.waitForJobCompletion(offlineUserDataJobService, jobResourceName);
      
      return success;
    } catch (err) {
      logger.error(`Error during Google Ads upload: ${err.message}`);
      if (err.details) {
        logger.error(`Details: ${JSON.stringify(err.details)}`);
      }
      return false;
    }
  }

  /**
   * Upload a batch of operations to Google Ads with retry logic
   */
  async _uploadBatchWithRetry(service, resourceName, batch, batchIndex, totalBatches) {
    const maxRetries = this.config.API_RETRY_COUNT;
    const baseDelay = this.config.API_RETRY_DELAY_BASE * 1000; // convert to ms
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        logger.info(`Batch ${batchIndex}/${totalBatches}: Attempt ${attempt}/${maxRetries} to upload ${batch.length} operations`);
        
        await service.addOperations({
          resourceName: resourceName,
          operations: batch
        });
        
        logger.info(`Batch ${batchIndex}/${totalBatches}: Successfully uploaded on attempt ${attempt}`);
        return true;
      } catch (err) {
        const isRetryable = 
          err.message.includes('CONCURRENT_MODIFICATION') || 
          err.message.includes('RESOURCE_EXHAUSTED') ||
          err.message.includes('DEADLINE_EXCEEDED');
        
        if (isRetryable && attempt < maxRetries) {
          // Calculate exponential backoff delay with jitter
          const delay = baseDelay * Math.pow(2, attempt - 1) * (0.5 + Math.random() * 0.5);
          logger.warn(`Batch ${batchIndex}/${totalBatches}: Retryable error on attempt ${attempt}: ${err.message}`);
          logger.info(`Waiting ${delay}ms before retry...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        } else {
          if (attempt >= maxRetries) {
            logger.error(`Batch ${batchIndex}/${totalBatches}: Failed after ${maxRetries} attempts`);
          } else {
            logger.error(`Batch ${batchIndex}/${totalBatches}: Non-retryable error: ${err.message}`);
          }
          throw err;
        }
      }
    }
  }

  /**
   * Wait for a Google Ads offline user data job to complete
   */
  async waitForJobCompletion(service, jobResourceName, timeoutSeconds = 300) {
    const startTime = Date.now();
    const pollInterval = 10000; // 10 seconds
    
    logger.info(`Waiting up to ${timeoutSeconds} seconds for job to complete...`);
    
    while (Date.now() - startTime < timeoutSeconds * 1000) {
      try {
        const response = await service.get({
          resourceName: jobResourceName
        });
        
        const status = response.status;
        logger.info(`Current job status: ${status}`);
        
        if (status === 'SUCCESS') {
          logger.info(`Job completed successfully!`);
          return true;
        } else if (status === 'FAILED') {
          logger.error(`Job failed: ${response.failureReason || 'Unknown reason'}`);
          return false;
        } else if (status === 'PENDING' || status === 'RUNNING') {
          // Job still in progress, wait and check again
          await new Promise(resolve => setTimeout(resolve, pollInterval));
        } else {
          logger.warn(`Unknown job status: ${status}`);
          return false;
        }
      } catch (err) {
        logger.error(`Error checking job status: ${err.message}`);
        return false;
      }
    }
    
    logger.error(`Timeout waiting for job completion after ${timeoutSeconds} seconds`);
    return false;
  }

  /**
   * Run the complete customer data upload process
   */
  async run() {
    try {
      logger.info(`Starting Google Ads Customer Data Upload Tool - Run ID: ${this.runId}`);
      logger.info(`Mode: ${this.runMode}, Brand Filter: ${this.brand || 'All brands'}`);
      
      // Initialize connections
      logger.info("Initializing database connection...");
      await this.initializeDbConnection();
      
      // Fetch and process data
      logger.info("Fetching and processing customer data...");
      await this.fetchAndProcessCustomerData();
      
      // Display statistics
      logger.info("\n==== Customer Data Processing Statistics ====");
      logger.info(`Total records processed: ${this.totalRowsProcessed}`);
      logger.info(`Records with valid email: ${this.emailProcessedCount}`);
      logger.info(`Records with valid phone: ${this.phoneProcessedCount}`);
      logger.info(`Records with valid address: ${this.addressProcessedCount}`);
      logger.info(`Records with at least one valid identifier: ${this.rowsWithAnyIdCount}`);
      logger.info(`Operations prepared for upload: ${this.processedOperations.length}`);
      logger.info("=========================================\n");
      
      // If stats-only mode, don't proceed with upload
      if (argv['stats-only']) {
        logger.info("Stats-only mode enabled. Skipping Google Ads upload.");
        return true;
      }
      
      // Initialize Google Ads client and upload data
      logger.info("Initializing Google Ads client...");
      this.initializeGoogleAdsClient();
      
      // Upload data
      logger.info("Uploading processed data to Google Ads...");
      const success = await this.uploadToGoogleAds();
      
      if (success) {
        logger.info("Google Ads upload completed successfully!");
      } else {
        logger.error("Google Ads upload encountered errors. Check logs for details.");
      }
      
      return success;
    } catch (err) {
      logger.error(`Fatal error during execution: ${err.message}`);
      if (err.stack) {
        logger.error(err.stack);
      }
      return false;
    }
  }
}

/**
 * Main function to run the upload tool
 */
async function main() {
  try {
    // Regular single brand or all brands upload process
    const uploader = new GoogleAdsUploader(
      DEFAULT_CONFIG,
      argv.mode,
      argv.brand
    );
    
    const success = await uploader.run();
    return success ? 0 : 1;
  } catch (err) {
    logger.error(`Unhandled error in main: ${err.message}`);
    if (err.stack) {
      logger.error(err.stack);
    }
    return 1;
  }
}

// Run the main function
main().then(exitCode => {
  process.exit(exitCode);
}).catch(err => {
  logger.error(`Failed to execute main function: ${err.message}`);
  process.exit(1);
});