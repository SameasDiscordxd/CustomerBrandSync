require("dotenv").config();
const axios = require("axios");
const crypto = require("crypto");

// Test with single customer to debug the API call
async function testSingleUpload() {
  try {
    // Get access token
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
    console.log("✓ Access token obtained");

    // Test data
    const customerId = "4018470779";
    const listId = "9081148884"; // BBT_NON_CUSTOMER

    // Create a simple test customer
    const testCustomer = {
      userIdentifiers: [
        {
          hashedEmail: crypto
            .createHash("sha256")
            .update("test@example.com")
            .digest("hex"),
        },
      ],
    };

    console.log("Creating offline user data job...");

    // Step 1: Create job using the correct endpoint
    const createJobRequest = {
      job: {
        type: "CUSTOMER_MATCH_USER_LIST",
        customerMatchUserListMetadata: {
          userList: `customers/${customerId}/userLists/${listId}`,
        },
      },
    };

    console.log("Request payload:", JSON.stringify(createJobRequest, null, 2));

    const createJobResponse = await axios.post(
      `https://googleads.googleapis.com/v18/customers/${customerId}/offlineUserDataJobs`,
      createJobRequest,
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "developer-token": process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✓ Job created successfully");
    console.log("Response:", JSON.stringify(createJobResponse.data, null, 2));

    const jobResourceName = createJobResponse.data.resourceName;
    console.log("Job resource name:", jobResourceName);

    // Step 2: Add operations
    console.log("Adding operations...");

    const addOperationsRequest = {
      operations: [
        {
          create: testCustomer,
        },
      ],
      enablePartialFailure: true,
    };

    console.log(
      "Add operations payload:",
      JSON.stringify(addOperationsRequest, null, 2)
    );

    const addResponse = await axios.post(
      `https://googleads.googleapis.com/v18/${jobResourceName}:addOperations`,
      addOperationsRequest,
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "developer-token": process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✓ Operations added successfully");
    console.log("Add response:", JSON.stringify(addResponse.data, null, 2));

    // Step 3: Run job
    console.log("Running job...");

    const runResponse = await axios.post(
      `https://googleads.googleapis.com/v18/${jobResourceName}:run`,
      {},
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "developer-token": process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✓ Job started successfully");
    console.log("Run response:", JSON.stringify(runResponse.data, null, 2));
  } catch (err) {
    console.error("❌ Error:", err.message);
    if (err.response) {
      console.error("Status:", err.response.status);
      console.error(
        "Response data:",
        JSON.stringify(err.response.data, null, 2)
      );
    }
  }
}

testSingleUpload();
