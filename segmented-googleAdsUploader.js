require('dotenv').config();
const { GoogleAdsApi } = require('google-ads-api');
const sql = require('mssql');
const crypto = require('crypto');
const winston = require('winston');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
const { parsePhoneNumber } = require('libphonenumber-js');

/**
 * Default configuration
 * These can be overridden by .env file or command line arguments
 */
const defaultConfig = {
  // Database connection
  DB_SERVER: process.env.DB_SERVER || 'localhost',
  DB_NAME: process.env.DB_NAME || 'Venom',
  DB_USER: process.env.DB_USER || '',
  DB_PASSWORD: process.env.DB_PASSWORD || '',

  // Google Ads API credentials
  GOOGLE_ADS_CLIENT_ID: process.env.GOOGLE_ADS_CLIENT_ID,
  GOOGLE_ADS_CLIENT_SECRET: process.env.GOOGLE_ADS_CLIENT_SECRET,
  GOOGLE_ADS_DEVELOPER_TOKEN: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
  GOOGLE_ADS_REFRESH_TOKEN: process.env.GOOGLE_ADS_REFRESH_TOKEN,
  GOOGLE_ADS_CUSTOMER_ID: process.env.GOOGLE_ADS_CUSTOMER_ID,

  // BBT Brand Lists
  BBT_ALL_USER_LIST_ID: process.env.BBT_ALL_USER_LIST_ID,
  BBT_TIRE_USER_LIST_ID: process.env.BBT_TIRE_USER_LIST_ID,
  BBT_SERVICE_USER_LIST_ID: process.env.BBT_SERVICE_USER_LIST_ID,
  BBT_LAPSED_USER_LIST_ID: process.env.BBT_LAPSED_USER_LIST_ID,
  BBT_NON_CUSTOMER_USER_LIST_ID: process.env.BBT_NON_CUSTOMER_USER_LIST_ID,
  BBT_REPEAT_USER_LIST_ID: process.env.BBT_REPEAT_USER_LIST_ID,

  // ATD Brand Lists
  ATD_ALL_USER_LIST_ID: process.env.ATD_ALL_USER_LIST_ID,
  ATD_TIRE_USER_LIST_ID: process.env.ATD_TIRE_USER_LIST_ID,
  ATD_SERVICE_USER_LIST_ID: process.env.ATD_SERVICE_USER_LIST_ID,
  ATD_LAPSED_USER_LIST_ID: process.env.ATD_LAPSED_USER_LIST_ID,
  ATD_NON_CUSTOMER_USER_LIST_ID: process.env.ATD_NON_CUSTOMER_USER_LIST_ID,
  ATD_REPEAT_USER_LIST_ID: process.env.ATD_REPEAT_USER_LIST_ID,

  // TW Brand Lists
  TW_ALL_USER_LIST_ID: process.env.TW_ALL_USER_LIST_ID,
  TW_TIRE_USER_LIST_ID: process.env.TW_TIRE_USER_LIST_ID,
  TW_SERVICE_USER_LIST_ID: process.env.TW_SERVICE_USER_LIST_ID,
  TW_LAPSED_USER_LIST_ID: process.env.TW_LAPSED_USER_LIST_ID,
  TW_NON_CUSTOMER_USER_LIST_ID: process.env.TW_NON_CUSTOMER_USER_LIST_ID,
  TW_REPEAT_USER_LIST_ID: process.env.TW_REPEAT_USER_LIST_ID
};

/**
 * Command line argument configuration
 */
const argv = yargs(hideBin(process.argv))
  .usage('Usage: $0 [options]')
  .option('mode', {
    alias: 'm',
    describe: 'Upload mode: delta (incremental) or full (complete replacement)',
    choices: ['delta', 'full'],
    default: 'delta'
  })
  .option('brand', {
    alias: 'b', 
    describe: 'Specific brand to upload (BBT, ATD, TW). If not specified, uploads all brands.',
    type: 'string'
  })
  .option('segment', {
    alias: 's',
    describe: 'Specific segment to upload (ALL, TIRE, SERVICE, LAPSED, NON_CUSTOMER, REPEAT)',
    choices: ['ALL', 'TIRE', 'SERVICE', 'LAPSED', 'NON_CUSTOMER', 'REPEAT']
  })
  .option('dry-run', {
    alias: 'd',
    describe: 'Test run without actually uploading to Google Ads',
    type: 'boolean',
    default: false
  })
  .help('h')
  .alias('h', 'help')
  .example('$0 --mode delta', 'Run incremental upload for all brands and segments')
  .example('$0 --mode full --brand BBT', 'Run full upload for BBT brand only')
  .example('$0 --brand ATD --segment TIRE', 'Upload only tire customers for ATD brand')
  .argv;

// Set up logging
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'google-ads-upload.log' })
  ]
});

/**
 * Google Ads Segmented Uploader class
 */
class GoogleAdsSegmentedUploader {
  constructor(config, runMode = 'delta', targetBrand = null, targetSegment = null) {
    this.config = config;
    this.runMode = runMode;
    this.targetBrand = targetBrand;
    this.targetSegment = targetSegment;
    this.totalRowsProcessed = 0;
    this.runId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    
    // Map SQL brand names to our internal codes
    this.brandMapping = {
      'Big Brand Tire': 'BBT',
      'American Tire Depot': 'ATD',
      'Tire World': 'TW',
      'Robertson Tire': 'RT',
      'Tires To You': 'TTY',
      '': 'UNKNOWN',  // Handle blank preferred brand
      'default': 'UNKNOWN'
    };

    // Store the user list IDs for each brand segment
    this.brandLists = {
      // BBT Lists
      'BBT_ALL': config.BBT_ALL_USER_LIST_ID,
      'BBT_TIRE': config.BBT_TIRE_USER_LIST_ID,
      'BBT_SERVICE': config.BBT_SERVICE_USER_LIST_ID,
      'BBT_LAPSED': config.BBT_LAPSED_USER_LIST_ID,
      'BBT_NON_CUSTOMER': config.BBT_NON_CUSTOMER_USER_LIST_ID,
      'BBT_REPEAT': config.BBT_REPEAT_USER_LIST_ID,
      
      // ATD Lists
      'ATD_ALL': config.ATD_ALL_USER_LIST_ID,
      'ATD_TIRE': config.ATD_TIRE_USER_LIST_ID,
      'ATD_SERVICE': config.ATD_SERVICE_USER_LIST_ID,
      'ATD_LAPSED': config.ATD_LAPSED_USER_LIST_ID,
      'ATD_NON_CUSTOMER': config.ATD_NON_CUSTOMER_USER_LIST_ID,
      'ATD_REPEAT': config.ATD_REPEAT_USER_LIST_ID,
      
      // TW Lists
      'TW_ALL': config.TW_ALL_USER_LIST_ID,
      'TW_TIRE': config.TW_TIRE_USER_LIST_ID,
      'TW_SERVICE': config.TW_SERVICE_USER_LIST_ID,
      'TW_LAPSED': config.TW_LAPSED_USER_LIST_ID,
      'TW_NON_CUSTOMER': config.TW_NON_CUSTOMER_USER_LIST_ID,
      'TW_REPEAT': config.TW_REPEAT_USER_LIST_ID
    };

    // Track operations by list for all segments
    this.operationsByList = {};
    Object.keys(this.brandLists).forEach(listKey => {
      this.operationsByList[listKey] = [];
    });
    
    // Log configured lists
    logger.info('Configured audience lists:');
    Object.entries(this.brandLists).forEach(([listName, listId]) => {
      if (listId) logger.info(`${listName}: ${listId}`);
    });
  }

  /**
   * Initialize the database connection
   */
  async initializeDbConnection() {
    const dbConfig = {
      server: this.config.DB_SERVER,
      database: this.config.DB_NAME,
      user: this.config.DB_USER,
      password: this.config.DB_PASSWORD,
      options: {
        encrypt: true,
        trustServerCertificate: true,
        enableArithAbort: true,
        requestTimeout: 300000,
        connectionTimeout: 30000
      },
      pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
      }
    };

    try {
      this.dbConn = await sql.connect(dbConfig);
      logger.info('Database connection established successfully');
      return true;
    } catch (err) {
      logger.error(`Database connection failed: ${err.message}`);
      throw err;
    }
  }

  /**
   * Initialize the Google Ads API client
   */
  initializeGoogleAdsClient() {
    try {
      this.googleAdsClient = new GoogleAdsApi({
        client_id: this.config.GOOGLE_ADS_CLIENT_ID,
        client_secret: this.config.GOOGLE_ADS_CLIENT_SECRET,
        developer_token: this.config.GOOGLE_ADS_DEVELOPER_TOKEN
      });

      this.customer = this.googleAdsClient.Customer({
        customer_id: this.config.GOOGLE_ADS_CUSTOMER_ID,
        refresh_token: this.config.GOOGLE_ADS_REFRESH_TOKEN
      });

      logger.info('Google Ads API client initialized successfully');
      return true;
    } catch (err) {
      logger.error(`Google Ads API client initialization failed: ${err.message}`);
      throw err;
    }
  }

  /**
   * Fetch customer data using the new segmentation query
   */
  async fetchAndProcessCustomerData() {
    try {
      logger.info('Fetching segmented customer data from database...');
      
      const request = this.dbConn.request();
      request.input('FullUpload', sql.Bit, this.runMode === 'full' ? 1 : 0);
      
      // Use the comprehensive segmentation query
      const query = `
        WITH CustomerBase AS (
          SELECT DISTINCT
              c.CustomerNumber,
              c.FirstName,
              c.LastName,
              c.ContactGUID,
              ih.CustomerEmail,
              ih.CustomerPhoneNumber,
              ih.CustomerZipCode,
              ih.CustomerState AS StateCode,
              COALESCE(sb.Name, 'default') AS BrandId,
              ih.InvoicedDate,
              ii.PartTypeId,
              id.PartNumber,
              CASE WHEN ii.PartTypeId = 13688 THEN 1 ELSE 0 END AS IsTirePurchase,
              CASE WHEN LEFT(id.PartNumber, 3) = 'LAB' OR (ii.PartTypeId IS NOT NULL AND ii.PartTypeId != 13688) THEN 1 ELSE 0 END AS IsServicePurchase
          FROM dbo.Customer AS c
          INNER JOIN dbo.InvoiceHeader AS ih ON c.Id = ih.CustomerId
          INNER JOIN dbo.InvoiceDetail AS id ON ih.Id = id.InvoiceHeaderId
          LEFT JOIN dbo.Store AS s ON ih.StoreId = s.Id
          LEFT JOIN dbo.StoreBrand AS sb ON s.StoreBrandId = sb.Id
          LEFT JOIN dbo.InventoryItem AS ii ON id.ItemId = ii.ItemId
          WHERE 
              ((ih.CustomerEmail IS NOT NULL AND ih.CustomerEmail <> '')
              OR (ih.CustomerPhoneNumber IS NOT NULL AND ih.CustomerPhoneNumber <> ''))
              AND ih.StatusId = 3
              AND id.Active = 1
              AND id.Approved = 1
              AND COALESCE(sb.Name, 'default') IN ('Big Brand Tire', 'American Tire Depot', 'Tire World', 'Robertson Tire', 'Tires To You')
              AND (@FullUpload = 1 OR c.AddDate > DATEADD(day, -30, GETDATE()) OR c.ChangeDate > DATEADD(day, -30, GETDATE()))
        ),
        CustomerSegments AS (
          SELECT 
              CustomerNumber, FirstName, LastName, ContactGUID,
              CustomerEmail, CustomerPhoneNumber, CustomerZipCode, StateCode, BrandId,
              MAX(IsTirePurchase) AS IsTireCustomer,
              MAX(IsServicePurchase) AS IsServiceCustomer,
              MAX(InvoicedDate) AS LastPurchaseDate,
              COUNT(DISTINCT InvoicedDate) AS TotalVisits,
              DATEDIFF(MONTH, MAX(InvoicedDate), GETDATE()) AS MonthsSinceLastPurchase,
              CASE WHEN COUNT(DISTINCT InvoicedDate) > 1 THEN 1 ELSE 0 END AS IsRepeatCustomer,
              CASE WHEN DATEDIFF(MONTH, MAX(InvoicedDate), GETDATE()) >= 15 THEN 1 ELSE 0 END AS IsLapsedCustomer
          FROM CustomerBase
          GROUP BY CustomerNumber, FirstName, LastName, ContactGUID, 
                   CustomerEmail, CustomerPhoneNumber, CustomerZipCode, StateCode, BrandId
        ),
        NonCustomers AS (
          SELECT DISTINCT
              'Non-Customer' AS CustomerNumber,
              mc.FirstName, mc.LastName, NULL AS ContactGUID,
              mc.Email AS CustomerEmail, NULL AS CustomerPhoneNumber,
              NULL AS CustomerZipCode, NULL AS StateCode,
              COALESCE(mc.PreferredBrand, 'default') AS BrandId,
              0 AS IsTireCustomer, 0 AS IsServiceCustomer,
              NULL AS LastPurchaseDate, 0 AS TotalVisits,
              NULL AS MonthsSinceLastPurchase,
              0 AS IsRepeatCustomer, 0 AS IsLapsedCustomer, 1 AS IsNonCustomer
          FROM dbo.MailChimp mc
          WHERE mc.MC_Status = 'subscribed' 
            AND (mc.VenomCustomerId = 0 OR mc.VenomCustomerId IS NULL)
            AND (mc.PreferredBrand IN ('Big Brand Tire', 'American Tire Depot', 'Tire World', 'Robertson Tire', 'Tires To You') 
                 OR mc.PreferredBrand IS NULL OR mc.PreferredBrand = '')
        )
        
        SELECT *, 0 AS IsNonCustomer FROM CustomerSegments
        UNION ALL
        SELECT *, IsNonCustomer FROM NonCustomers
        ORDER BY BrandId, CustomerNumber
      `;

      const result = await request.query(query);
      
      if (!result.recordset || result.recordset.length === 0) {
        logger.warn('No customer data found');
        return 0;
      }

      this.totalRowsProcessed = result.recordset.length;
      logger.info(`Processing ${this.totalRowsProcessed} customer records`);

      // Process each customer and assign to appropriate lists
      for (const row of result.recordset) {
        await this._processCustomerRow(row);
      }

      // Log segment counts
      this._logSegmentCounts();
      
      return this.totalRowsProcessed;
    } catch (err) {
      logger.error(`Error fetching customer data: ${err.message}`);
      throw err;
    }
  }

  /**
   * Process a single customer row and assign to appropriate segments
   */
  async _processCustomerRow(row) {
    try {
      const operation = this._createGoogleAdsOperation(
        row.CustomerEmail,
        row.CustomerPhoneNumber,
        row.FirstName,
        row.LastName,
        row.CustomerZipCode,
        row.StateCode
      );

      if (!operation) return;

      const brandCode = this.brandMapping[row.BrandId];
      if (!brandCode) return;

      // Apply brand and segment filters if specified
      if (this.targetBrand && brandCode !== this.targetBrand) return;

      // Add to ALL customers list for this brand
      if (!this.targetSegment || this.targetSegment === 'ALL') {
        this.operationsByList[`${brandCode}_ALL`].push(operation);
      }

      // Add to specific segment lists based on customer characteristics
      if (row.IsTireCustomer && (!this.targetSegment || this.targetSegment === 'TIRE')) {
        this.operationsByList[`${brandCode}_TIRE`].push(operation);
      }

      if (row.IsServiceCustomer && (!this.targetSegment || this.targetSegment === 'SERVICE')) {
        this.operationsByList[`${brandCode}_SERVICE`].push(operation);
      }

      if (row.IsLapsedCustomer && (!this.targetSegment || this.targetSegment === 'LAPSED')) {
        this.operationsByList[`${brandCode}_LAPSED`].push(operation);
      }

      if (row.IsNonCustomer && (!this.targetSegment || this.targetSegment === 'NON_CUSTOMER')) {
        this.operationsByList[`${brandCode}_NON_CUSTOMER`].push(operation);
      }

      if (row.IsRepeatCustomer && (!this.targetSegment || this.targetSegment === 'REPEAT')) {
        this.operationsByList[`${brandCode}_REPEAT`].push(operation);
      }

    } catch (err) {
      logger.error(`Error processing customer row: ${err.message}`);
    }
  }

  /**
   * Log segment counts for verification
   */
  _logSegmentCounts() {
    logger.info('\n=== SEGMENT COUNTS ===');
    Object.entries(this.operationsByList).forEach(([listKey, operations]) => {
      if (operations.length > 0) {
        logger.info(`${listKey}: ${operations.length} customers`);
      }
    });
    logger.info('=====================\n');
  }

  /**
   * Normalize and hash string for Google Ads
   */
  _normalizeAndHashString(input) {
    if (!input || typeof input !== 'string') return null;
    
    const normalized = input.toLowerCase().trim();
    return crypto.createHash('sha256').update(normalized).digest('hex');
  }

  /**
   * Create a Google Ads operation from customer data
   */
  _createGoogleAdsOperation(email, phone, firstName, lastName, zipCode, stateCode) {
    const userIdentifiers = [];
    
    // Add hashed email if available
    if (email && email.trim()) {
      const hashedEmail = this._normalizeAndHashString(email.trim());
      if (hashedEmail) {
        userIdentifiers.push({
          hashed_email: hashedEmail
        });
      }
    }
    
    // Add hashed phone if available and valid
    if (phone && phone.trim()) {
      try {
        const phoneNumber = parsePhoneNumber(phone.trim(), 'US');
        if (phoneNumber && phoneNumber.isValid()) {
          const hashedPhone = this._normalizeAndHashString(phoneNumber.format('E.164'));
          if (hashedPhone) {
            userIdentifiers.push({
              hashed_phone_number: hashedPhone
            });
          }
        }
      } catch (err) {
        // Invalid phone number, skip
      }
    }
    
    // Add address information if available
    if (firstName || lastName || zipCode || stateCode) {
      const addressInfo = {};
      
      if (firstName) addressInfo.hashed_first_name = this._normalizeAndHashString(firstName.trim());
      if (lastName) addressInfo.hashed_last_name = this._normalizeAndHashString(lastName.trim());
      if (zipCode) addressInfo.postal_code = zipCode.trim();
      if (stateCode) addressInfo.country_code = 'US';
      
      if (Object.keys(addressInfo).length > 0) {
        userIdentifiers.push({
          address_info: addressInfo
        });
      }
    }
    
    if (userIdentifiers.length === 0) {
      return null;
    }
    
    return {
      create: {
        user_identifiers: userIdentifiers
      }
    };
  }

  /**
   * Upload operations to Google Ads for all configured segments
   */
  async uploadToGoogleAds() {
    if (argv['dry-run']) {
      logger.info('DRY RUN MODE - No actual uploads will be performed');
      this._logSegmentCounts();
      return true;
    }

    let overallSuccess = true;
    
    for (const [listKey, operations] of Object.entries(this.operationsByList)) {
      if (operations.length === 0) continue;
      
      const listId = this.brandLists[listKey];
      if (!listId) {
        logger.warn(`No list ID configured for ${listKey}, skipping`);
        continue;
      }
      
      logger.info(`\n=== Uploading ${operations.length} operations to ${listKey} (ID: ${listId}) ===`);
      
      const success = await this._uploadToSpecificList(listKey, listId, operations);
      if (!success) {
        logger.error(`Failed to upload to ${listKey} list`);
        overallSuccess = false;
      } else {
        logger.info(`Successfully uploaded to ${listKey} list`);
        await this._updateTrackingRecord(listKey, listId, operations.length, true);
      }
    }
    
    return overallSuccess;
  }

  /**
   * Upload operations to a specific list
   */
  async _uploadToSpecificList(listName, listId, operations) {
    try {
      const service = this.customer.offlineUserDataJobs();
      const resourceName = `customers/${this.config.GOOGLE_ADS_CUSTOMER_ID}/offlineUserDataJobs`;
      
      // Create offline user data job
      const jobResponse = await service.create({
        customer_id: this.config.GOOGLE_ADS_CUSTOMER_ID,
        offline_user_data_job: {
          type: 'CUSTOMER_MATCH_USER_LIST',
          customer_match_user_list_metadata: {
            user_list: `customers/${this.config.GOOGLE_ADS_CUSTOMER_ID}/userLists/${listId}`
          }
        }
      });
      
      const jobResourceName = jobResponse.resource_name;
      logger.info(`Created offline user data job: ${jobResourceName}`);
      
      // Upload operations in batches
      const batchSize = 1000;
      const totalBatches = Math.ceil(operations.length / batchSize);
      
      for (let i = 0; i < totalBatches; i++) {
        const start = i * batchSize;
        const end = Math.min(start + batchSize, operations.length);
        const batch = operations.slice(start, end);
        
        const success = await this._uploadBatchWithRetry(service, jobResourceName, batch, i + 1, totalBatches);
        if (!success) {
          logger.error(`Failed to upload batch ${i + 1}/${totalBatches}`);
          return false;
        }
      }
      
      // Run the job
      await service.run({ resource_name: jobResourceName });
      logger.info(`Started job execution for ${listName}`);
      
      // Wait for completion
      const completed = await this.waitForJobCompletion(service, jobResourceName);
      if (!completed) {
        logger.error(`Job did not complete successfully for ${listName}`);
        return false;
      }
      
      logger.info(`Successfully completed upload to ${listName}`);
      return true;
      
    } catch (err) {
      logger.error(`Error uploading to ${listName}: ${err.message}`);
      return false;
    }
  }

  /**
   * Upload a batch with retry logic
   */
  async _uploadBatchWithRetry(service, jobResourceName, batch, batchIndex, totalBatches) {
    const maxRetries = 3;
    let attempt = 0;
    
    while (attempt < maxRetries) {
      try {
        await service.addOperations({
          resource_name: jobResourceName,
          operations: batch
        });
        
        logger.info(`Uploaded batch ${batchIndex}/${totalBatches} (${batch.length} operations)`);
        return true;
        
      } catch (err) {
        attempt++;
        logger.warn(`Batch ${batchIndex} upload attempt ${attempt} failed: ${err.message}`);
        
        if (attempt >= maxRetries) {
          logger.error(`Failed to upload batch ${batchIndex} after ${maxRetries} attempts`);
          return false;
        }
        
        // Wait before retry
        await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
      }
    }
    
    return false;
  }

  /**
   * Wait for job completion
   */
  async waitForJobCompletion(service, jobResourceName, timeoutSeconds = 300) {
    const startTime = Date.now();
    
    while (Date.now() - startTime < timeoutSeconds * 1000) {
      try {
        const job = await service.get({ resource_name: jobResourceName });
        
        if (job.status === 'SUCCESS') {
          logger.info('Job completed successfully');
          return true;
        } else if (job.status === 'FAILED') {
          logger.error('Job failed');
          return false;
        }
        
        logger.info(`Job status: ${job.status || 'PENDING'}, waiting...`);
        await new Promise(resolve => setTimeout(resolve, 10000));
        
      } catch (err) {
        logger.error(`Error checking job status: ${err.message}`);
        await new Promise(resolve => setTimeout(resolve, 10000));
      }
    }
    
    logger.error(`Timeout waiting for job completion after ${timeoutSeconds} seconds`);
    return false;
  }

  /**
   * Update tracking table
   */
  async _updateTrackingRecord(listName, listId, operationsCount, success) {
    if (!this.dbConn) {
      logger.warn("Database connection not available for tracking update");
      return;
    }

    try {
      const [brandCode, segment] = listName.split('_');
      const brandName = Object.keys(this.brandMapping).find(key => this.brandMapping[key] === brandCode);

      const request = this.dbConn.request();
      
      const description = this.runMode === 'full' 
        ? `Full Customer Upload - ${brandName} ${segment}` 
        : `Delta Customer Upload - ${brandName} ${segment}`;

      const insertQuery = `
        INSERT INTO dbo.GoogleAdsUploadTracking 
        (LastUploadDate, UploadDescription, RowsProcessed, SuccessFlag, ActualUploadedCount, BrandName, BrandListId, BrandRowsProcessed)
        VALUES (GETDATE(), @description, @rowsProcessed, @successFlag, @actualCount, @brandName, @listId, @brandRowsProcessed)
      `;

      request.input('description', description);
      request.input('rowsProcessed', this.totalRowsProcessed);
      request.input('successFlag', success ? 1 : 0);
      request.input('actualCount', operationsCount);
      request.input('brandName', brandName);
      request.input('listId', listId);
      request.input('brandRowsProcessed', operationsCount);

      await request.query(insertQuery);
      logger.info(`Tracking record updated for ${listName}: ${operationsCount} operations uploaded`);
      
    } catch (err) {
      logger.error(`Error updating tracking record for ${listName}: ${err.message}`);
    }
  }

  /**
   * Run the complete upload process
   */
  async run() {
    try {
      logger.info(`Starting Google Ads Segmented Upload Tool - Run ID: ${this.runId}`);
      logger.info(`Mode: ${this.runMode}`);
      if (this.targetBrand) logger.info(`Target Brand: ${this.targetBrand}`);
      if (this.targetSegment) logger.info(`Target Segment: ${this.targetSegment}`);
      
      // Initialize connections
      await this.initializeDbConnection();
      this.initializeGoogleAdsClient();
      
      // Fetch and process data
      const rowsProcessed = await this.fetchAndProcessCustomerData();
      if (rowsProcessed === 0) {
        logger.warn('No data to upload');
        return true;
      }
      
      // Upload to Google Ads
      const success = await this.uploadToGoogleAds();
      
      if (success) {
        logger.info(`✓ Upload completed successfully. Processed ${rowsProcessed} records.`);
      } else {
        logger.error(`✗ Upload completed with errors. Processed ${rowsProcessed} records.`);
      }
      
      return success;
      
    } catch (err) {
      logger.error(`Upload process failed: ${err.message}`);
      throw err;
    } finally {
      if (this.dbConn) {
        await this.dbConn.close();
        logger.info('Database connection closed');
      }
    }
  }
}

/**
 * Main function
 */
async function main() {
  try {
    // Validate required configuration
    const missingFields = [];
    const requiredFields = [
      'GOOGLE_ADS_CLIENT_ID', 'GOOGLE_ADS_CLIENT_SECRET', 
      'GOOGLE_ADS_DEVELOPER_TOKEN', 'GOOGLE_ADS_REFRESH_TOKEN', 'GOOGLE_ADS_CUSTOMER_ID',
      'DB_SERVER', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'
    ];

    requiredFields.forEach(field => {
      if (!defaultConfig[field]) {
        missingFields.push(field);
      }
    });

    if (missingFields.length > 0) {
      logger.error(`Missing required configuration: ${missingFields.join(', ')}`);
      logger.error('Please check your .env file or environment variables');
      process.exit(1);
    }

    // Create and run uploader
    const uploader = new GoogleAdsSegmentedUploader(
      defaultConfig, 
      argv.mode, 
      argv.brand ? argv.brand.toUpperCase() : null,
      argv.segment
    );
    
    const success = await uploader.run();
    process.exit(success ? 0 : 1);
    
  } catch (err) {
    logger.error(`Application error: ${err.message}`);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  main();
}

module.exports = { GoogleAdsSegmentedUploader };