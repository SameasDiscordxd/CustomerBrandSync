require("dotenv").config();
const axios = require("axios");
const crypto = require("crypto");

// Mirror your working Python script exactly
async function pythonMirrorUpload() {
  try {
    console.log("🚀 Starting Python-mirrored upload...");

    // Step 1: Get access token (same as Python)
    console.log("Getting access token...");
    const tokenResponse = await axios.post(
      "https://oauth2.googleapis.com/token",
      {
        client_id: process.env.GOOGLE_ADS_CLIENT_ID,
        client_secret: process.env.GOOGLE_ADS_CLIENT_SECRET,
        refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN,
        grant_type: "refresh_token",
      }
    );

    const accessToken = tokenResponse.data.access_token;
    console.log("✅ Access token obtained");

    // Test configuration
    const customerId = "4018470779"; // BBT
    const listId = "9081148884"; // BBT_NON_CUSTOMER

    // Create headers like your Python script
    const headers = {
      Authorization: `Bearer ${accessToken}`,
      "developer-token": process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
      "Content-Type": "application/json",
    };

    console.log("📋 Headers:", headers);

    // Step 1: Create job - try different endpoint formats
    const endpoints = [
      `https://googleads.googleapis.com/v17/customers/${customerId}/offlineUserDataJobs:mutate`,
      `https://googleads.googleapis.com/v16/customers/${customerId}/offlineUserDataJobs:mutate`,
      `https://googleads.googleapis.com/v15/customers/${customerId}/offlineUserDataJobs:mutate`,
      `https://googleads.googleapis.com/v14/customers/${customerId}/offlineUserDataJobs:mutate`,
      `https://googleads.googleapis.com/v13/customers/${customerId}/offlineUserDataJobs:mutate`,
    ];

    let jobResourceName = null;
    let workingEndpoint = null;

    for (const endpoint of endpoints) {
      try {
        console.log(`🔍 Trying endpoint: ${endpoint}`);

        const createJobPayload = {
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

        console.log(
          "📤 Request payload:",
          JSON.stringify(createJobPayload, null, 2)
        );

        const response = await axios.post(endpoint, createJobPayload, {
          headers,
        });

        console.log("✅ SUCCESS with endpoint:", endpoint);
        console.log("📥 Response:", JSON.stringify(response.data, null, 2));

        jobResourceName = response.data.results[0].resourceName;
        workingEndpoint = endpoint.replace(":mutate", "");
        break;
      } catch (err) {
        console.log(
          `❌ Failed with ${endpoint}: ${err.response?.status} ${err.message}`
        );
        if (err.response?.data) {
          console.log(
            "Error details:",
            JSON.stringify(err.response.data, null, 2)
          );
        }
      }
    }

    if (!jobResourceName) {
      throw new Error("❌ All endpoints failed!");
    }

    console.log(`🎯 Working endpoint found: ${workingEndpoint}`);
    console.log(`📝 Job created: ${jobResourceName}`);

    // Step 2: Add operations (mirror Python)
    console.log("📊 Adding test customer data...");

    const testOperation = {
      create: {
        userIdentifiers: [
          {
            hashedEmail: crypto
              .createHash("sha256")
              .update("test@example.com")
              .digest("hex"),
          },
        ],
      },
    };

    const addOperationsPayload = {
      operations: [testOperation],
      enablePartialFailure: true,
    };

    console.log(
      "📤 Add operations payload:",
      JSON.stringify(addOperationsPayload, null, 2)
    );

    const addResponse = await axios.post(
      `${workingEndpoint}/${jobResourceName}:addOperations`,
      addOperationsPayload,
      { headers }
    );

    console.log("✅ Operations added successfully");
    console.log("📥 Add response:", JSON.stringify(addResponse.data, null, 2));

    // Step 3: Run job (mirror Python)
    console.log("🚀 Running job...");

    const runResponse = await axios.post(
      `${workingEndpoint}/${jobResourceName}:run`,
      {},
      { headers }
    );

    console.log("✅ Job started successfully");
    console.log("📥 Run response:", JSON.stringify(runResponse.data, null, 2));

    console.log("🎉 COMPLETE SUCCESS! Working configuration found:");
    console.log(`   • Endpoint: ${workingEndpoint}`);
    console.log(`   • Job: ${jobResourceName}`);
    console.log(`   • This matches your Python script approach!`);
  } catch (error) {
    console.error("💥 Fatal error:", error.message);
    if (error.response) {
      console.error("Status:", error.response.status);
      console.error("Response:", JSON.stringify(error.response.data, null, 2));
    }
  }
}

pythonMirrorUpload();
