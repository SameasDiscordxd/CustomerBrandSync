require("dotenv").config();
const sql = require("mssql");
const crypto = require("crypto");
const winston = require("winston");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");
const { parsePhoneNumber } = require("libphonenumber-js");
const axios = require("axios");

class SimpleGoogleAdsUploader {
  constructor() {
    this.config = {
      // Database configuration
      DB_SERVER:
        process.env.DB_SERVER ||
        "bbtdatanew.privatelink.westus.cloudapp.azure.com",
      DB_PORT: process.env.DB_PORT || "1401",
      DB_NAME: process.env.DB_DATABASE || process.env.DB_NAME || "Venom",
      DB_USER: process.env.DB_USERNAME || process.env.DB_USER || "APIUser",
      DB_PASSWORD: process.env.DB_PASSWORD,

      // Google Ads API configuration
      GOOGLE_ADS_CLIENT_ID: process.env.GOOGLE_ADS_CLIENT_ID,
      GOOGLE_ADS_CLIENT_SECRET: process.env.GOOGLE_ADS_CLIENT_SECRET,
      GOOGLE_ADS_DEVELOPER_TOKEN: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
      GOOGLE_ADS_REFRESH_TOKEN: process.env.GOOGLE_ADS_REFRESH_TOKEN,
      // Added for Manager Account (MCC) access
      GOOGLE_ADS_LOGIN_CUSTOMER_ID: process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID,

      // Customer IDs
      BBT_CUSTOMER_ID: process.env.BBT_CUSTOMER_ID || "4018470779",
      ATD_CUSTOMER_ID: process.env.ATD_CUSTOMER_ID || "7258331724",
      TW_CUSTOMER_ID: process.env.TW_CUSTOMER_ID || "9883819925",

      // Audience List IDs
      BBT_ALL_USER_LIST_ID: process.env.BBT_ALL_USER_LIST_ID || "9080505530",
      BBT_TIRE_USER_LIST_ID: process.env.BBT_TIRE_USER_LIST_ID || "9080506007",
      BBT_SERVICE_USER_LIST_ID:
        process.env.BBT_SERVICE_USER_LIST_ID || "9079539925",
      BBT_LAPSED_USER_LIST_ID:
        process.env.BBT_LAPSED_USER_LIST_ID || "9080507210",
      BBT_NON_CUSTOMER_USER_LIST_ID:
        process.env.BBT_NON_CUSTOMER_USER_LIST_ID || "9081148884",
      BBT_REPEAT_USER_LIST_ID:
        process.env.BBT_REPEAT_USER_LIST_ID || "9081149361",

      ATD_ALL_USER_LIST_ID: process.env.ATD_ALL_USER_LIST_ID || "9081149991",
      ATD_TIRE_USER_LIST_ID: process.env.ATD_TIRE_USER_LIST_ID || "9079542079",
      ATD_SERVICE_USER_LIST_ID:
        process.env.ATD_SERVICE_USER_LIST_ID || "9080509355",
      ATD_LAPSED_USER_LIST_ID:
        process.env.ATD_LAPSED_USER_LIST_ID || "9081150972",
      ATD_NON_CUSTOMER_USER_LIST_ID:
        process.env.ATD_NON_CUSTOMER_USER_LIST_ID || "9081151308",
      ATD_REPEAT_USER_LIST_ID:
        process.env.ATD_REPEAT_USER_LIST_ID || "9081151668",

      TW_ALL_USER_LIST_ID: process.env.TW_ALL_USER_LIST_ID || "9079545817",
      TW_TIRE_USER_LIST_ID: process.env.TW_TIRE_USER_LIST_ID || "9079546213",
      TW_SERVICE_USER_LIST_ID:
        process.env.TW_SERVICE_USER_LIST_ID || "9081154926",
      TW_LAPSED_USER_LIST_ID:
        process.env.TW_LAPSED_USER_LIST_ID || "9080514113",
      TW_NON_CUSTOMER_USER_LIST_ID:
        process.env.TW_NON_CUSTOMER_USER_LIST_ID || "9080514200",
      TW_REPEAT_USER_LIST_ID:
        process.env.TW_REPEAT_USER_LIST_ID || "9079547371",
    };

    this.brandMapping = {
      "Big Brand Tire": "BBT",
      "American Tire Depot": "ATD",
      "Tire World": "TW",
    };

    this.brandLists = {
      BBT: {
        ALL: this.config.BBT_ALL_USER_LIST_ID,
        TIRE: this.config.BBT_TIRE_USER_LIST_ID,
        SERVICE: this.config.BBT_SERVICE_USER_LIST_ID,
        LAPSED: this.config.BBT_LAPSED_USER_LIST_ID,
        NON_CUSTOMER: this.config.BBT_NON_CUSTOMER_USER_LIST_ID,
        REPEAT: this.config.BBT_REPEAT_USER_LIST_ID,
      },
      ATD: {
        ALL: this.config.ATD_ALL_USER_LIST_ID,
        TIRE: this.config.ATD_TIRE_USER_LIST_ID,
        SERVICE: this.config.ATD_SERVICE_USER_LIST_ID,
        LAPSED: this.config.ATD_LAPSED_USER_LIST_ID,
        NON_CUSTOMER: this.config.ATD_NON_CUSTOMER_USER_LIST_ID,
        REPEAT: this.config.ATD_REPEAT_USER_LIST_ID,
      },
      TW: {
        ALL: this.config.TW_ALL_USER_LIST_ID,
        TIRE: this.config.TW_TIRE_USER_LIST_ID,
        SERVICE: this.config.TW_SERVICE_USER_LIST_ID,
        LAPSED: this.config.TW_LAPSED_USER_LIST_ID,
        NON_CUSTOMER: this.config.TW_NON_CUSTOMER_USER_LIST_ID,
        REPEAT: this.config.TW_REPEAT_USER_LIST_ID,
      },
    };

    this.customers = {
      BBT: this.config.BBT_CUSTOMER_ID,
      ATD: this.config.ATD_CUSTOMER_ID,
      TW: this.config.TW_CUSTOMER_ID,
    };

    this.runId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    this.dbPool = null;
    this.accessToken = null;
  }

  async initializeDbConnection() {
    try {
      let server, port;

      if (this.config.DB_SERVER.includes(":")) {
        const serverParts = this.config.DB_SERVER.split(":");
        server = serverParts[0];
        port = parseInt(serverParts[1]);
      } else {
        server = this.config.DB_SERVER;
        port = parseInt(this.config.DB_PORT);
      }

      logger.info(`Connecting to server: ${server}, port: ${port}`);

      this.dbPool = new sql.ConnectionPool({
        server: server,
        port: port,
        database: this.config.DB_NAME,
        user: this.config.DB_USER,
        password: this.config.DB_PASSWORD,
        options: {
          encrypt: true,
          trustServerCertificate: true,
          enableArithAbort: true,
          requestTimeout: 300000,
        },
        pool: {
          max: 10,
          min: 0,
          idleTimeoutMillis: 30000,
        },
      });

      await this.dbPool.connect();
      logger.info("Database connection established successfully");
      return true;
    } catch (err) {
      logger.error(`Database connection failed: ${err.message}`);
      throw err;
    }
  }

  async getAccessToken() {
    try {
      const response = await axios.post("https://oauth2.googleapis.com/token", {
        client_id: this.config.GOOGLE_ADS_CLIENT_ID,
        client_secret: this.config.GOOGLE_ADS_CLIENT_SECRET,
        refresh_token: this.config.GOOGLE_ADS_REFRESH_TOKEN,
        grant_type: "refresh_token",
      });

      this.accessToken = response.data.access_token;
      logger.info("Access token obtained successfully");
      return this.accessToken;
    } catch (err) {
      logger.error(`Failed to get access token: ${err.message}`);
      if (err.response) {
        logger.error(
          `OAuth Error Details: ${JSON.stringify(err.response.data, null, 2)}`
        );
      }
      throw err;
    }
  }

  async fetchAndProcessCustomerData(fullUpload = false) {
    try {
      logger.info("Fetching segmented customer data from database...");
      const request = new sql.Request(this.dbPool);
      request.input("FullUpload", sql.Bit, fullUpload);

      const result = await request.execute(
        "dbo.GetNewCustomersForGoogleAdsWithBrandInfo"
      );
      const customerData = result.recordset;

      logger.info(`Processing ${customerData.length} customer records`);

      const segmentedCustomers = {
        BBT: {
          ALL: [],
          TIRE: [],
          SERVICE: [],
          LAPSED: [],
          NON_CUSTOMER: [],
          REPEAT: [],
        },
        ATD: {
          ALL: [],
          TIRE: [],
          SERVICE: [],
          LAPSED: [],
          NON_CUSTOMER: [],
          REPEAT: [],
        },
        TW: {
          ALL: [],
          TIRE: [],
          SERVICE: [],
          LAPSED: [],
          NON_CUSTOMER: [],
          REPEAT: [],
        },
      };

      for (const row of customerData) {
        const brandCode = this.brandMapping[row.BrandId];
        if (!brandCode) continue;

        const operation = this._createGoogleAdsOperation(row);
        if (!operation) continue;

        segmentedCustomers[brandCode].ALL.push(operation);
        if (row.IsTireCustomer)
          segmentedCustomers[brandCode].TIRE.push(operation);
        if (row.IsServiceCustomer)
          segmentedCustomers[brandCode].SERVICE.push(operation);
        if (row.IsLapsedCustomer)
          segmentedCustomers[brandCode].LAPSED.push(operation);
        if (row.IsNonCustomer)
          segmentedCustomers[brandCode].NON_CUSTOMER.push(operation);
        if (row.IsRepeatCustomer)
          segmentedCustomers[brandCode].REPEAT.push(operation);
      }

      this._logSegmentCounts(segmentedCustomers);
      return segmentedCustomers;
    } catch (err) {
      logger.error(`Data processing failed: ${err.message}`);
      throw err;
    }
  }

  _createGoogleAdsOperation(row) {
    try {
      const userData = { userIdentifiers: [] };

      if (row.CustomerEmail && row.CustomerEmail.trim()) {
        userData.userIdentifiers.push({
          hashedEmail: this._hashData(row.CustomerEmail.toLowerCase().trim()),
        });
      }

      if (row.CustomerPhoneNumber && row.CustomerPhoneNumber.trim()) {
        try {
          const phoneNumber = parsePhoneNumber(row.CustomerPhoneNumber, "US");
          if (phoneNumber && phoneNumber.isValid()) {
            userData.userIdentifiers.push({
              hashedPhoneNumber: this._hashData(phoneNumber.format("E.164")),
            });
          }
        } catch (phoneErr) {
          // Skip invalid phone numbers
        }
      }

      if (row.CustomerZipCode && row.StateCode) {
        userData.userIdentifiers.push({
          addressInfo: {
            hashedPostalCode: this._hashData(row.CustomerZipCode.trim()),
            countryCode: "US",
            hashedRegionCode: this._hashData(
              row.StateCode.trim().toUpperCase()
            ),
          },
        });
      }
      return userData.userIdentifiers.length > 0 ? { create: userData } : null;
    } catch (err) {
      logger.error(`Error creating operation for customer: ${err.message}`);
      return null;
    }
  }

  _hashData(data) {
    return crypto.createHash("sha256").update(data).digest("hex");
  }

  _logSegmentCounts(segmentedCustomers) {
    logger.info("\n=== SEGMENT COUNTS ===");
    for (const [brand, segments] of Object.entries(segmentedCustomers)) {
      for (const [segment, customers] of Object.entries(segments)) {
        if (customers.length > 0) {
          logger.info(`${brand}_${segment}: ${customers.length} customers`);
        }
      }
    }
    logger.info("=====================");
  }

  async uploadToGoogleAds(
    segmentedCustomers,
    isDryRun = false,
    targetBrand = null,
    targetSegment = null
  ) {
    if (isDryRun) {
      logger.info("DRY RUN MODE - No actual uploads will be performed");
      this._logSegmentCounts(segmentedCustomers);
      return { success: true, uploaded: 0 };
    }

    let totalUploaded = 0;
    let errors = [];

    for (const [brand, segments] of Object.entries(segmentedCustomers)) {
      if (targetBrand && brand !== targetBrand) continue;
      for (const [segment, operations] of Object.entries(segments)) {
        if (targetSegment && segment !== targetSegment) continue;
        if (operations.length === 0) continue;

        const listId = this.brandLists[brand][segment];
        const customerId = this.customers[brand];

        if (!listId || !customerId) {
          logger.warn(`Missing configuration for ${brand}_${segment}`);
          continue;
        }

        try {
          logger.info(
            `\n=== Uploading ${operations.length} operations to ${brand}_${segment} (ID: ${listId}) ===`
          );
          await this._uploadToSpecificList(
            customerId,
            listId,
            operations,
            `${brand}_${segment}`
          );
          totalUploaded += operations.length;
          await this._updateTrackingRecord(
            `${brand}_${segment}`,
            operations.length,
            true
          );
        } catch (err) {
          logger.error(
            `Error uploading to ${brand}_${segment}: ${err.message}`
          );
          errors.push(`${brand}_${segment}: ${err.message}`);
          await this._updateTrackingRecord(
            `${brand}_${segment}`,
            operations.length,
            false
          );
        }
      }
    }
    return { success: errors.length === 0, uploaded: totalUploaded, errors };
  }

  async _uploadToSpecificList(customerId, listId, operations, segmentName) {
    try {
      logger.info(
        `Starting upload to ${segmentName} with ${operations.length} operations`
      );

      // Define base headers
      const requestHeaders = {
        Authorization: `Bearer ${this.accessToken}`,
        "developer-token": this.config.GOOGLE_ADS_DEVELOPER_TOKEN,
        "Content-Type": "application/json",
      };

      // Conditionally add login-customer-id header
      if (this.config.GOOGLE_ADS_LOGIN_CUSTOMER_ID) {
        requestHeaders["login-customer-id"] =
          this.config.GOOGLE_ADS_LOGIN_CUSTOMER_ID;
        logger.info(
          `Using Login Customer ID (MCC): ${this.config.GOOGLE_ADS_LOGIN_CUSTOMER_ID} for requests to customer ${customerId}.`
        );
      } else {
        logger.info(
          `Login Customer ID (MCC) not configured. Making requests directly to customer ${customerId}.`
        );
      }

      // Step 1: Create the job
      logger.info(`Creating job for customer ${customerId}, list ${listId}`);
      const createJobRequest = {
        operations: [
          {
            create: {
              type: "CUSTOMER_MATCH_USER_LIST",
              customerMatchUserListMetadata: {
                userList: `customers/${customerId}/userLists/${listId}`,
              },
            },
          },
        ],
      };
      logger.info(
        `Job creation request payload: ${JSON.stringify(
          createJobRequest,
          null,
          2
        )}`
      );

      const createJobResponse = await axios.post(
        `https://googleads.googleapis.com/v18/customers/${customerId}/offlineUserDataJobs:mutate`,
        createJobRequest,
        { headers: requestHeaders }
      );

      if (
        !createJobResponse.data.results ||
        createJobResponse.data.results.length === 0
      ) {
        throw new Error(
          "Failed to create offline user data job - no results returned"
        );
      }
      const jobResourceName = createJobResponse.data.results[0].resourceName;
      logger.info(`Created job: ${jobResourceName}`);

      // Step 2: Add operations in batches
      const batchSize = parseInt(process.env.API_BATCH_SIZE) || 2500; // Use env var or default
      const batches = [];
      for (let i = 0; i < operations.length; i += batchSize) {
        batches.push(operations.slice(i, i + batchSize));
      }
      logger.info(
        `Uploading ${operations.length} operations in ${batches.length} batches of size ${batchSize}`
      );

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        logger.info(
          `✓ Uploading batch ${i + 1}/${batches.length} (${
            batch.length
          } operations)`
        );
        const addOperationsPayload = {
          operations: batch,
          enablePartialFailure: true,
        };
        await axios.post(
          `https://googleads.googleapis.com/v18/${jobResourceName}:addOperations`,
          addOperationsPayload,
          { headers: requestHeaders }
        );
        if (i < batches.length - 1) {
          const delay = parseFloat(process.env.API_RATE_LIMIT_DELAY) || 0.5;
          await new Promise((resolve) => setTimeout(resolve, delay * 1000)); // delay in ms
        }
      }

      // Step 3: Run the job
      logger.info(`Starting job execution for ${segmentName}`);
      await axios.post(
        `https://googleads.googleapis.com/v18/${jobResourceName}:run`,
        {},
        { headers: requestHeaders }
      );
      logger.info(
        "Job submitted for processing. Google Ads will process asynchronously."
      );
      logger.info(
        `✓ Successfully completed upload to ${segmentName}: ${operations.length} customers`
      );
      return true;
    } catch (err) {
      logger.error(`Upload failed for ${segmentName}: ${err.message}`);
      if (err.response) {
        logger.error(`Status: ${err.response.status}`);
        logger.error(
          `Response Headers: ${JSON.stringify(err.response.headers, null, 2)}`
        );
        logger.error(
          `API Error Details: ${JSON.stringify(err.response.data, null, 2)}`
        );
      } else if (err.request) {
        logger.error(
          "Error: No response received. Request details:",
          err.request
        );
      } else {
        logger.error("Error: Request setup issue.", err.message);
      }
      logger.error(
        `Stack trace for ${segmentName} upload failure: ${err.stack}`
      );
      throw err;
    }
  }

  async _updateTrackingRecord(segmentName, operationCount, success) {
    try {
      if (!this.dbPool) return;
      const request = new sql.Request(this.dbPool);
      request.input("LastUploadDate", sql.DateTime, new Date());
      request.input(
        "UploadDescription",
        sql.VarChar(255),
        `Google Ads Upload - ${segmentName} - Run ID: ${this.runId}`
      );
      request.input("RowsProcessed", sql.Int, operationCount);
      request.input("SuccessFlag", sql.Bit, success);
      request.input(
        "ActualUploadedCount",
        sql.Int,
        success ? operationCount : 0
      );
      await request.query(
        `INSERT INTO dbo.GoogleAdsUploadTracking 
         (LastUploadDate, UploadDescription, RowsProcessed, SuccessFlag, ActualUploadedCount)
         VALUES (@LastUploadDate, @UploadDescription, @RowsProcessed, @SuccessFlag, @ActualUploadedCount)`
      );
      logger.info(
        `Tracking record updated for ${segmentName}: ${operationCount} operations, Success: ${success}`
      );
    } catch (err) {
      logger.error(`Failed to update tracking record: ${err.message}`);
    }
  }

  async run(
    mode = "delta",
    targetBrand = null,
    targetSegment = null,
    isDryRun = false
  ) {
    try {
      const fullUpload = mode === "full";
      logger.info(
        `Connecting to database: ${this.config.DB_SERVER}:${this.config.DB_PORT}`
      );
      await this.initializeDbConnection();
      await this.getAccessToken();

      const segmentedCustomers = await this.fetchAndProcessCustomerData(
        fullUpload
      );
      const totalRecords = Object.values(segmentedCustomers)
        .flatMap((brand) => Object.values(brand))
        .reduce((sum, segment) => sum + segment.length, 0);

      const result = await this.uploadToGoogleAds(
        segmentedCustomers,
        isDryRun,
        targetBrand,
        targetSegment
      );

      if (result.success) {
        logger.info(
          `✓ ${
            isDryRun ? "Dry run" : "Upload"
          } completed successfully. Potential operations based on DB fetch: ${totalRecords}. Actual attempted uploads (filtered by brand/segment): ${
            result.uploaded
          }.`
        );
      } else {
        logger.error(
          `✗ Upload completed with errors. Potential operations based on DB fetch: ${totalRecords}.`
        );
        if (result.errors.length > 0) {
          logger.error(`Specific errors: ${result.errors.join(", ")}`);
        }
      }
      return result.success;
    } catch (err) {
      logger.error(`Upload process failed: ${err.message}`);
      logger.error(`Stack trace: ${err.stack}`);
      return false;
    } finally {
      if (this.dbPool) {
        try {
          await this.dbPool.close();
          logger.info("Database connection closed");
        } catch (closeErr) {
          logger.error(
            `Error closing database connection: ${closeErr.message}`
          );
        }
      }
    }
  }
}

const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "google-ads-upload.log" }),
  ],
});

const argv = yargs(hideBin(process.argv))
  .option("mode", {
    alias: "m",
    type: "string",
    description: "Upload mode: 'delta' (default) or 'full'",
    default: "delta",
    choices: ["delta", "full"],
  })
  .option("brand", {
    alias: "b",
    type: "string",
    description: "Target specific brand: BBT, ATD, or TW",
    choices: ["BBT", "ATD", "TW"],
  })
  .option("segment", {
    alias: "s",
    type: "string",
    description:
      "Target specific segment: ALL, TIRE, SERVICE, LAPSED, NON_CUSTOMER, REPEAT",
    choices: ["ALL", "TIRE", "SERVICE", "LAPSED", "NON_CUSTOMER", "REPEAT"],
  })
  .option("dry-run", {
    alias: "d",
    type: "boolean",
    description: "Perform a dry run without uploading",
    default: false,
  })
  .help()
  .alias("help", "h").argv;

async function main() {
  const uploader = new SimpleGoogleAdsUploader();
  logger.info("Configured audience lists:");
  Object.entries(uploader.brandLists).forEach(([brand, segments]) => {
    Object.entries(segments).forEach(([segment, listId]) => {
      logger.info(`${brand}_${segment}: ${listId}`);
    });
  });
  if (uploader.config.GOOGLE_ADS_LOGIN_CUSTOMER_ID) {
    logger.info(
      `Using GOOGLE_ADS_LOGIN_CUSTOMER_ID (MCC): ${uploader.config.GOOGLE_ADS_LOGIN_CUSTOMER_ID}`
    );
  }

  logger.info(
    `Starting Simple Google Ads Upload Tool - Run ID: ${uploader.runId}`
  );
  logger.info(`Mode: ${argv.mode}`);
  if (argv.brand) logger.info(`Target Brand: ${argv.brand}`);
  if (argv.segment) logger.info(`Target Segment: ${argv.segment}`);

  try {
    const success = await uploader.run(
      argv.mode,
      argv.brand,
      argv.segment,
      argv["dry-run"]
    );
    process.exit(success ? 0 : 1);
  } catch (err) {
    logger.error(`Fatal error in main execution: ${err.message}`);
    logger.error(`Stack trace: ${err.stack}`);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = SimpleGoogleAdsUploader;
