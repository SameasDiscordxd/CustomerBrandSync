require("dotenv").config();
const { GoogleAdsApi } = require("google-ads-api");

async function inspectGoogleAdsClient() {
  try {
    console.log("🔍 Detailed Google Ads Client Inspection");
    console.log("=====================================");

    // Initialize client
    const client = new GoogleAdsApi({
      client_id: process.env.GOOGLE_ADS_CLIENT_ID,
      client_secret: process.env.GOOGLE_ADS_CLIENT_SECRET,
      developer_token: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
    });

    console.log("\n1. CLIENT OBJECT INSPECTION:");
    console.log("Client type:", typeof client);
    console.log("Client constructor:", client.constructor.name);
    console.log("Client properties:", Object.getOwnPropertyNames(client));
    console.log("Client keys:", Object.keys(client));

    // Check for services on the client
    if (client.services) {
      console.log("\n2. CLIENT SERVICES:");
      console.log("Services type:", typeof client.services);
      console.log("Available services:", Object.keys(client.services));
    }

    // Create customer
    const customer = client.Customer({
      customer_id: process.env.BBT_CUSTOMER_ID || "4018470779",
      refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN,
    });

    console.log("\n3. CUSTOMER OBJECT INSPECTION:");
    console.log("Customer type:", typeof customer);
    console.log("Customer constructor:", customer.constructor.name);

    // Get all properties and methods
    const allProps = Object.getOwnPropertyNames(customer);
    const methods = allProps.filter((prop) => {
      try {
        return typeof customer[prop] === "function";
      } catch (e) {
        return false;
      }
    });
    const properties = allProps.filter((prop) => {
      try {
        return typeof customer[prop] !== "function";
      } catch (e) {
        return false;
      }
    });

    console.log("Customer methods:", methods);
    console.log("Customer properties:", properties);

    // Check specific methods we expect
    const expectedMethods = [
      "offlineUserDataJobs",
      "mutate",
      "query",
      "report",
      "reportStream",
      "search",
      "searchStream",
    ];

    console.log("\n4. EXPECTED METHODS CHECK:");
    expectedMethods.forEach((method) => {
      const exists = typeof customer[method] === "function";
      console.log(
        `${exists ? "✅" : "❌"} ${method}: ${exists ? "EXISTS" : "MISSING"}`
      );

      if (exists) {
        try {
          console.log(`   ${method} type:`, typeof customer[method]);
          console.log(
            `   ${method} toString:`,
            customer[method].toString().substring(0, 100) + "..."
          );
        } catch (e) {
          console.log(`   Error inspecting ${method}:`, e.message);
        }
      }
    });

    // Test basic query to confirm client works
    console.log("\n5. BASIC FUNCTIONALITY TEST:");
    try {
      const queryResult = await customer.query(`
        SELECT customer.id, customer.descriptive_name 
        FROM customer 
        LIMIT 1
      `);
      console.log("✅ Basic query works. Result:", queryResult);
    } catch (err) {
      console.log("❌ Basic query failed:", err.message);
    }

    // Check for services on customer
    console.log("\n6. CUSTOMER SERVICES CHECK:");
    if (customer.services) {
      console.log("Customer services type:", typeof customer.services);
      console.log("Customer services keys:", Object.keys(customer.services));

      // Look for offline user data job service
      const oudjs =
        customer.services.OfflineUserDataJobService ||
        customer.services.offlineUserDataJobService ||
        customer.services.OfflineUserDataJobs ||
        customer.services.offlineUserDataJobs;

      if (oudjs) {
        console.log("✅ Found offline user data job service!");
        console.log("Service type:", typeof oudjs);
        console.log("Service methods:", Object.getOwnPropertyNames(oudjs));
      } else {
        console.log("❌ No offline user data job service found");
        console.log("Available services:", Object.keys(customer.services));
      }
    } else {
      console.log("❌ No services property on customer");
    }

    // Check the client for services
    console.log("\n7. DIRECT CLIENT SERVICES:");
    if (client.OfflineUserDataJobService) {
      console.log("✅ Found OfflineUserDataJobService on client");
    } else {
      console.log("❌ No OfflineUserDataJobService on client");
    }

    // Try to call offlineUserDataJobs method if it exists
    if (typeof customer.offlineUserDataJobs === "function") {
      console.log("\n8. TESTING OFFLINE USER DATA JOBS METHOD:");
      try {
        // Try to call it with minimal parameters to see what happens
        console.log("Attempting to call offlineUserDataJobs...");
        const result = await customer.offlineUserDataJobs();
        console.log("✅ Method call succeeded:", result);
      } catch (err) {
        console.log("❌ Method call failed:", err.message);
        console.log("Error details:", err);
      }
    }

    console.log("\n🎯 INSPECTION COMPLETE!");
  } catch (error) {
    console.error("💥 Inspection failed:", error.message);
    console.error("Stack:", error.stack);
  }
}

inspectGoogleAdsClient();
