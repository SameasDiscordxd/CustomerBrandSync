require("dotenv").config();
const sql = require("mssql");
const crypto = require("crypto");
const winston = require("winston");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");
const { parsePhoneNumber } = require("libphonenumber-js");
const { GoogleAdsApi } = require("google-ads-api");

class WorkingGoogleAdsUploader {
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

      // List IDs
      BBT_NON_CUSTOMER_USER_LIST_ID:
        process.env.BBT_NON_CUSTOMER_USER_LIST_ID || "9081148884",
    };

    this.runId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    this.dbPool = null;
    this.googleAdsClient = null;
    this.customer = null;
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

  initializeGoogleAdsClient() {
    try {
      logger.info("Initializing Google Ads client library...");

      this.googleAdsClient = new GoogleAdsApi({
        client_id: this.config.GOOGLE_ADS_CLIENT_ID,
        client_secret: this.config.GOOGLE_ADS_CLIENT_SECRET,
        developer_token: this.config.GOOGLE_ADS_DEVELOPER_TOKEN,
      });

      this.customer = this.googleAdsClient.Customer({
        customer_id: this.config.BBT_CUSTOMER_ID,
        refresh_token: this.config.GOOGLE_ADS_REFRESH_TOKEN,
      });

      logger.info("Google Ads client initialized successfully");
      return true;
    } catch (err) {
      logger.error(`Google Ads client initialization failed: ${err.message}`);
      throw err;
    }
  }

  async testSingleUpload() {
    try {
      logger.info("🧪 Testing single customer upload...");

      // Create test customer data
      const testCustomer = {
        user_identifiers: [
          {
            hashed_email: crypto
              .createHash("sha256")
              .update("test@example.com")
              .digest("hex"),
          },
        ],
      };

      logger.info("Test customer data:", JSON.stringify(testCustomer, null, 2));

      // Try different approaches to create offline user data job
      const approaches = [
        // Approach 1: Direct service call
        async () => {
          logger.info("🔍 Trying approach 1: Direct service call");
          const response = await this.customer.offlineUserDataJobs({
            operations: [
              {
                create: {
                  type: "CUSTOMER_MATCH_USER_LIST",
                  customer_match_user_list_metadata: {
                    user_list: `customers/${this.config.BBT_CUSTOMER_ID}/userLists/${this.config.BBT_NON_CUSTOMER_USER_LIST_ID}`,
                  },
                },
              },
            ],
          });
          return response;
        },

        // Approach 2: Mutate method
        async () => {
          logger.info("🔍 Trying approach 2: Mutate method");
          const response = await this.customer.mutate([
            {
              offline_user_data_job_operation: {
                create: {
                  type: "CUSTOMER_MATCH_USER_LIST",
                  customer_match_user_list_metadata: {
                    user_list: `customers/${this.config.BBT_CUSTOMER_ID}/userLists/${this.config.BBT_NON_CUSTOMER_USER_LIST_ID}`,
                  },
                },
              },
            },
          ]);
          return response;
        },

        // Approach 3: Query method to check available services
        async () => {
          logger.info("🔍 Trying approach 3: List available services");
          const response = await this.customer.query(`
            SELECT customer.id, customer.descriptive_name 
            FROM customer 
            LIMIT 1
          `);
          logger.info("Customer query successful:", response);
          return { test: "query_works" };
        },

        // Approach 4: Check customer object methods
        async () => {
          logger.info("🔍 Trying approach 4: Inspect customer object");
          logger.info("Customer object type:", typeof this.customer);
          logger.info(
            "Customer object constructor:",
            this.customer.constructor.name
          );

          // Log available methods
          const methods = Object.getOwnPropertyNames(this.customer).filter(
            (prop) => typeof this.customer[prop] === "function"
          );
          logger.info("Available customer methods:", methods);

          return { test: "inspection_complete", methods };
        },
      ];

      for (let i = 0; i < approaches.length; i++) {
        try {
          logger.info(`\n--- Testing Approach ${i + 1} ---`);
          const result = await approaches[i]();
          logger.info(
            `✅ Approach ${i + 1} SUCCESS:`,
            JSON.stringify(result, null, 2)
          );

          if (i === 2) continue; // Skip query test, just for verification
          if (i === 3) continue; // Skip inspection, just for info

          // If we got here, this approach worked
          logger.info(`🎯 WORKING APPROACH FOUND: ${i + 1}`);
          return result;
        } catch (err) {
          logger.error(`❌ Approach ${i + 1} failed:`, err.message);
          if (err.response) {
            logger.error(
              "Error response:",
              JSON.stringify(err.response, null, 2)
            );
          }
        }
      }

      throw new Error("All approaches failed");
    } catch (err) {
      logger.error(`Test upload failed: ${err.message}`);
      throw err;
    }
  }

  async run() {
    try {
      logger.info(
        `🚀 Starting Working Google Ads Upload Test - Run ID: ${this.runId}`
      );

      // Connect to database
      logger.info(
        `Connecting to database: ${this.config.DB_SERVER}:${this.config.DB_PORT}`
      );
      await this.initializeDbConnection();

      // Initialize Google Ads client
      if (!this.initializeGoogleAdsClient()) return false;

      // Test single upload
      const result = await this.testSingleUpload();

      logger.info("✅ Test completed successfully");
      logger.info("Result:", JSON.stringify(result, null, 2));

      return true;
    } catch (err) {
      logger.error(`Upload test failed: ${err.message}`);
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
  transports: [new winston.transports.Console()],
});

// Main execution
async function main() {
  const uploader = new WorkingGoogleAdsUploader();

  try {
    const success = await uploader.run();
    process.exit(success ? 0 : 1);
  } catch (err) {
    logger.error(`Fatal error: ${err.message}`);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = WorkingGoogleAdsUploader;
