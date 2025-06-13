require("dotenv").config();
const sql = require("mssql");
const crypto = require("crypto");
const winston = require("winston");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");
const { parsePhoneNumber } = require("libphonenumber-js");
const fs = require("fs").promises;
const { spawn } = require("child_process");
const path = require("path");

class HybridGoogleAdsUploader {
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

        const customerRecord = this._createCustomerRecord(row);
        if (!customerRecord) continue;

        // Add to ALL segment for everyone
        segmentedCustomers[brandCode].ALL.push(customerRecord);

        // Add to specific segments based on flags
        if (row.IsTireCustomer)
          segmentedCustomers[brandCode].TIRE.push(customerRecord);
        if (row.IsServiceCustomer)
          segmentedCustomers[brandCode].SERVICE.push(customerRecord);
        if (row.IsLapsedCustomer)
          segmentedCustomers[brandCode].LAPSED.push(customerRecord);
        if (row.IsNonCustomer)
          segmentedCustomers[brandCode].NON_CUSTOMER.push(customerRecord);
        if (row.IsRepeatCustomer)
          segmentedCustomers[brandCode].REPEAT.push(customerRecord);
      }

      this._logSegmentCounts(segmentedCustomers);
      return segmentedCustomers;
    } catch (err) {
      logger.error(`Data processing failed: ${err.message}`);
      throw err;
    }
  }

  _createCustomerRecord(row) {
    try {
      const record = {};
      let hasData = false;

      // Add email if present
      if (row.CustomerEmail && row.CustomerEmail.trim()) {
        record.email = row.CustomerEmail.toLowerCase().trim();
        hasData = true;
      }

      // Add phone if present
      if (row.CustomerPhoneNumber && row.CustomerPhoneNumber.trim()) {
        try {
          const phoneNumber = parsePhoneNumber(row.CustomerPhoneNumber, "US");
          if (phoneNumber && phoneNumber.isValid()) {
            record.phone = phoneNumber.format("E.164");
            hasData = true;
          }
        } catch (phoneErr) {
          // Skip invalid phone numbers
        }
      }

      // Add address info if available
      if (row.CustomerZipCode && row.StateCode) {
        record.postal_code = row.CustomerZipCode.trim();
        record.country_code = "US";
        record.region_code = row.StateCode.trim().toUpperCase();
        hasData = true;
      }

      return hasData ? record : null;
    } catch (err) {
      logger.error(`Error creating customer record: ${err.message}`);
      return null;
    }
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

      for (const [segment, customers] of Object.entries(segments)) {
        if (targetSegment && segment !== targetSegment) continue;
        if (customers.length === 0) continue;

        const listId = this.brandLists[brand][segment];
        const customerId = this.customers[brand];

        if (!listId || !customerId) {
          logger.warn(`Missing configuration for ${brand}_${segment}`);
          continue;
        }

        try {
          logger.info(
            `\n=== Uploading ${customers.length} customers to ${brand}_${segment} (ID: ${listId}) ===`
          );
          await this._uploadUsingPython(
            customerId,
            listId,
            customers,
            `${brand}_${segment}`
          );
          totalUploaded += customers.length;

          // Track in database
          await this._updateTrackingRecord(
            `${brand}_${segment}`,
            customers.length,
            true
          );
        } catch (err) {
          logger.error(
            `Error uploading to ${brand}_${segment}: ${err.message}`
          );
          errors.push(`${brand}_${segment}: ${err.message}`);

          // Track failed upload
          await this._updateTrackingRecord(
            `${brand}_${segment}`,
            customers.length,
            false
          );
        }
      }
    }

    return { success: errors.length === 0, uploaded: totalUploaded, errors };
  }

  async _uploadUsingPython(customerId, listId, customers, segmentName) {
    try {
      logger.info(`Creating CSV file for ${segmentName}...`);

      // Create temporary CSV file
      const csvFile = path.join(
        __dirname,
        `temp_${segmentName}_${Date.now()}.csv`
      );
      const csvData = this._createCSV(customers);
      await fs.writeFile(csvFile, csvData);

      logger.info(`Created CSV with ${customers.length} customers: ${csvFile}`);

      // Call your existing Python script
      logger.info(`Calling Python script for upload...`);

      const pythonResult = await this._callPythonScript(
        customerId,
        listId,
        csvFile,
        segmentName
      );

      logger.info(`✅ Python upload completed successfully for ${segmentName}`);

      // Clean up CSV file
      await fs.unlink(csvFile);

      return pythonResult;
    } catch (err) {
      logger.error(`Python upload failed for ${segmentName}: ${err.message}`);
      throw err;
    }
  }

  _createCSV(customers) {
    const headers = [
      "email",
      "phone",
      "postal_code",
      "country_code",
      "region_code",
    ];
    const rows = [headers.join(",")];

    for (const customer of customers) {
      const row = headers.map((header) => {
        const value = customer[header] || "";
        // Escape commas and quotes in CSV
        return value.includes(",") || value.includes('"')
          ? `"${value.replace(/"/g, '""')}"`
          : value;
      });
      rows.push(row.join(","));
    }

    return rows.join("\n");
  }

  async _callPythonScript(customerId, listId, csvFile, segmentName) {
    return new Promise((resolve, reject) => {
      // Adjust this path to your actual Python script
      const pythonScript = path.join(__dirname, "your_python_uploader.py");

      const args = [
        pythonScript,
        "--customer-id",
        customerId,
        "--list-id",
        listId,
        "--csv-file",
        csvFile,
        "--segment-name",
        segmentName,
      ];

      logger.info(`Executing: python ${args.join(" ")}`);

      const pythonProcess = spawn("python", args, {
        stdio: ["pipe", "pipe", "pipe"],
        env: {
          ...process.env,
          // Pass your credentials to Python
          GOOGLE_ADS_CLIENT_ID: this.config.GOOGLE_ADS_CLIENT_ID,
          GOOGLE_ADS_CLIENT_SECRET: this.config.GOOGLE_ADS_CLIENT_SECRET,
          GOOGLE_ADS_DEVELOPER_TOKEN: this.config.GOOGLE_ADS_DEVELOPER_TOKEN,
          GOOGLE_ADS_REFRESH_TOKEN: this.config.GOOGLE_ADS_REFRESH_TOKEN,
        },
      });

      let stdout = "";
      let stderr = "";

      pythonProcess.stdout.on("data", (data) => {
        const output = data.toString();
        stdout += output;
        logger.info(`Python: ${output.trim()}`);
      });

      pythonProcess.stderr.on("data", (data) => {
        const output = data.toString();
        stderr += output;
        logger.warn(`Python stderr: ${output.trim()}`);
      });

      pythonProcess.on("close", (code) => {
        if (code === 0) {
          logger.info(`✅ Python script completed successfully`);
          resolve({ stdout, stderr, code });
        } else {
          logger.error(`❌ Python script failed with code ${code}`);
          reject(new Error(`Python script failed: ${stderr || stdout}`));
        }
      });

      pythonProcess.on("error", (err) => {
        logger.error(`❌ Failed to start Python script: ${err.message}`);
        reject(err);
      });
    });
  }

  async _updateTrackingRecord(segmentName, operationCount, success) {
    try {
      if (!this.dbPool) return;

      const request = new sql.Request(this.dbPool);
      request.input("LastUploadDate", sql.DateTime, new Date());
      request.input(
        "UploadDescription",
        sql.VarChar(255),
        `Hybrid Upload - ${segmentName} - Run ID: ${this.runId}`
      );
      request.input("RowsProcessed", sql.Int, operationCount);
      request.input("SuccessFlag", sql.Bit, success);
      request.input(
        "ActualUploadedCount",
        sql.Int,
        success ? operationCount : 0
      );

      await request.query(`
        INSERT INTO dbo.GoogleAdsUploadTracking 
        (LastUploadDate, UploadDescription, RowsProcessed, SuccessFlag, ActualUploadedCount)
        VALUES (@LastUploadDate, @UploadDescription, @RowsProcessed, @SuccessFlag, @ActualUploadedCount)
      `);

      logger.info(
        `Tracking record updated for ${segmentName}: ${operationCount} operations`
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
          `✅ ${
            isDryRun ? "Dry run" : "Upload"
          } completed successfully. Processed ${totalRecords} records.`
        );
      } else {
        logger.error(
          `❌ Upload completed with errors. Processed ${totalRecords} records.`
        );
        if (result.errors.length > 0) {
          logger.error(`Errors: ${result.errors.join(", ")}`);
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

// Logger configuration
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
    new winston.transports.File({ filename: "hybrid-upload.log" }),
  ],
});

// CLI setup
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

// Main execution
async function main() {
  const uploader = new HybridGoogleAdsUploader();

  logger.info("Configured audience lists:");
  Object.entries(uploader.brandLists).forEach(([brand, segments]) => {
    Object.entries(segments).forEach(([segment, listId]) => {
      logger.info(`${brand}_${segment}: ${listId}`);
    });
  });

  logger.info(
    `Starting Hybrid Google Ads Upload Tool - Run ID: ${uploader.runId}`
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
    logger.error(`Fatal error: ${err.message}`);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = HybridGoogleAdsUploader;
