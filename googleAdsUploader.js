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
 * - Brand-specific targeting capabilities
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
  DEFAULT_USER_LIST_ID: process.env.DEFAULT_USER_LIST_ID || "9027934773",
  API_BATCH_SIZE: parseInt(process.env.API_BATCH_SIZE) || 2500,  
  API_RETRY_COUNT: parseInt(process.env.API_RETRY_COUNT) || 3,
  API_RETRY_DELAY_BASE: parseInt(process.env.API_RETRY_DELAY_BASE) || 2,
  API_RATE_LIMIT_DELAY: parseFloat(process.env.API_RATE_LIMIT_DELAY) || 0.5,
  // Brand-specific configuration
  BRANDS: {
    default: {
      USER_LIST_ID: process.env.DEFAULT_USER_LIST_ID || "9027934773",
      DESCRIPTION: "Default Brand Customer List"
    }
    // Additional brands will be loaded from environment variables
  }
};

// Load brand-specific configuration from environment variables
Object.keys(process.env).forEach(key => {
  const match = key.match(/^(.+)_USER_LIST_ID$/);
  if (match && match[1] !== 'DEFAULT') {
    const brandName = match[1].toLowerCase();
    if (!DEFAULT_CONFIG.BRANDS[brandName]) {
      DEFAULT_CONFIG.BRANDS[brandName] = {
        USER_LIST_ID: process.env[key],
        DESCRIPTION: `${brandName.charAt(0).toUpperCase() + brandName.slice(1)} Brand Customer List`
      };
    }
  }
});

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
    description: 'Upload data for a specific brand only',
    type: 'string'
  })
  .option('create-brand-list', {
    description: 'Create a new user list for the specified brand',
    type: 'string'
  })
  .option('list-brands', {
    description: 'List all available brand configurations',
    type: 'boolean'
  })
  .option('list-user-lists', {
    description: 'List all available customer match user lists',
    type: 'boolean'
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
    
    // Brand-specific configuration
    this.availableBrands = Object.keys(config.BRANDS);
    logger.info(`Available brands: ${this.availableBrands.join(', ')}`);
    
    if (brand && !this.availableBrands.includes(brand)) {
      logger.warn(`Specified brand '${brand}' not found in configuration. Using default.`);
      this.brand = "default";
    }
    
    // Set user list ID based on brand
    if (brand && this.availableBrands.includes(brand)) {
      this.userListId = config.BRANDS[brand].USER_LIST_ID;
      logger.info(`Using brand: ${brand}, User List ID: ${this.userListId}`);
    } else {
      this.userListId = config.DEFAULT_USER_LIST_ID;
      logger.info(`Using default User List ID: ${this.userListId}`);
    }
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
      if (this.brand && this.brand !== "default") {
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
      if (this.brand && this.brand !== "default" && brandRaw && brandRaw !== this.brand) {
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
      const userListService = customer.getService('UserListService');
      
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
   * Create a new user list for a specific brand
   */
  async createBrandSpecificUserList(brandName, description = null) {
    if (!this.googleAdsClient) {
      logger.error("Google Ads client not initialized.");
      return null;
    }
    
    try {
      logger.info(`Creating new user list for brand: ${brandName}`);
      
      // Create a Customer object
      const customer = this.googleAdsClient.Customer({
        customer_id: this.config.GADS_CUSTOMER_ID,
        refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN
      });
      
      // Get the UserListService
      const userListService = customer.getService('UserListService');
      
      // Create the user list
      const listDescription = description || `${brandName.charAt(0).toUpperCase() + brandName.slice(1)} Brand Customer List`;
      
      const userListOperation = {
        create: {
          name: `${brandName.charAt(0).toUpperCase() + brandName.slice(1)} Brand Customers - ${new Date().toISOString().split('T')[0]}`,
          description: listDescription,
          membershipLifeSpan: 10000, // Days, roughly 27 years
          customerMatchUploadKeyType: 'CONTACT_INFO', // Use contact info for matching
          crm_based_user_list: {
            app_id: "com.example.app" // Required placeholder
          }
        }
      };
      
      const response = await userListService.mutate({
        customerId: this.config.GADS_CUSTOMER_ID,
        operations: [userListOperation]
      });
      
      if (response && response.results && response.results.length > 0) {
        const newUserList = response.results[0];
        logger.info(`Successfully created user list: ${newUserList.resourceName}`);
        
        // Extract the user list ID from resource name
        const userListId = newUserList.resourceName.split('/').pop();
        
        return {
          resourceName: newUserList.resourceName,
          userListId: userListId
        };
      } else {
        logger.error("Failed to create user list: No results returned");
        return null;
      }
    } catch (err) {
      logger.error(`Error creating brand-specific user list: ${err.message}`);
      if (err.details) {
        logger.error(`Details: ${JSON.stringify(err.details)}`);
      }
      return null;
    }
  }

  /**
   * List all available Customer Match user lists
   */
  async listAvailableUserLists() {
    if (!this.googleAdsClient) {
      logger.error("Google Ads client not initialized.");
      return null;
    }
    
    try {
      logger.info("Listing available Customer Match user lists...");
      
      // Create a Customer object
      const customer = this.googleAdsClient.Customer({
        customer_id: this.config.GADS_CUSTOMER_ID,
        refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN
      });
      
      // Get the GoogleAdsService
      const googleAdsService = customer.getService('GoogleAdsService');
      
      // Query for user lists
      const query = `
        SELECT
          user_list.id,
          user_list.name,
          user_list.description,
          user_list.membership_life_span,
          user_list.size_for_display,
          user_list.size_for_search,
          user_list.type
        FROM user_list
        WHERE user_list.type = 'CRM_BASED'
        ORDER BY user_list.id
      `;
      
      const response = await googleAdsService.search({
        customerId: this.config.GADS_CUSTOMER_ID,
        query: query
      });
      
      const userLists = [];
      
      for await (const row of response) {
        userLists.push({
          id: row.userList.id,
          name: row.userList.name,
          description: row.userList.description,
          membershipLifeSpan: row.userList.membershipLifeSpan,
          sizeForDisplay: row.userList.sizeForDisplay,
          sizeForSearch: row.userList.sizeForSearch,
          type: row.userList.type
        });
      }
      
      return userLists;
    } catch (err) {
      logger.error(`Error listing user lists: ${err.message}`);
      if (err.details) {
        logger.error(`Details: ${JSON.stringify(err.details)}`);
      }
      return null;
    }
  }

  /**
   * Run the complete customer data upload process
   */
  async run() {
    try {
      logger.info(`Starting Google Ads Customer Data Upload Tool - Run ID: ${this.runId}`);
      logger.info(`Mode: ${this.runMode}, Brand: ${this.brand || 'All brands'}`);
      
      // Initialize connections
      logger.info("Initializing database connection...");
      await this.initializeDbConnection();
      
      logger.info("Initializing Google Ads client...");
      this.initializeGoogleAdsClient();
      
      // Fetch and process data
      logger.info("Fetching and processing customer data...");
      await this.fetchAndProcessCustomerData();
      
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
    // Handle list-brands command
    if (argv['list-brands']) {
      logger.info("Available brand configurations:");
      Object.keys(DEFAULT_CONFIG.BRANDS).forEach(brand => {
        logger.info(`  - ${brand}: User List ID ${DEFAULT_CONFIG.BRANDS[brand].USER_LIST_ID}`);
      });
      return 0;
    }
    
    // Handle create-brand-list command
    if (argv['create-brand-list']) {
      const brandName = argv['create-brand-list'].toLowerCase();
      
      // Initialize Google Ads client first
      const uploader = new GoogleAdsUploader(DEFAULT_CONFIG);
      uploader.initializeGoogleAdsClient();
      
      const result = await uploader.createBrandSpecificUserList(brandName);
      if (result) {
        logger.info(`Successfully created user list for brand '${brandName}'`);
        logger.info(`User List ID: ${result.userListId}`);
        logger.info(`Add to configuration with environment variable: ${brandName.toUpperCase()}_USER_LIST_ID=${result.userListId}`);
      } else {
        logger.error(`Failed to create user list for brand '${brandName}'`);
      }
      return result ? 0 : 1;
    }
    
    // Handle list-user-lists command
    if (argv['list-user-lists']) {
      // Initialize Google Ads client first
      const uploader = new GoogleAdsUploader(DEFAULT_CONFIG);
      uploader.initializeGoogleAdsClient();
      
      const userLists = await uploader.listAvailableUserLists();
      if (userLists && userLists.length > 0) {
        logger.info(`Found ${userLists.length} customer match user lists:`);
        userLists.forEach(list => {
          logger.info(`  - ID: ${list.id}, Name: ${list.name}`);
          logger.info(`    Description: ${list.description || 'None'}`);
          logger.info(`    Size: ${list.sizeForDisplay || 0} (display), ${list.sizeForSearch || 0} (search)`);
        });
      } else {
        logger.warn("No customer match user lists found.");
      }
      return 0;
    }
    
    // Regular upload process
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