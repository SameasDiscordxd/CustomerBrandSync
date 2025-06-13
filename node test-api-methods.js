require("dotenv").config();
const { GoogleAdsApi } = require("google-ads-api");

async function testApiMethods() {
  try {
    const client = new GoogleAdsApi({
      client_id: process.env.GOOGLE_ADS_CLIENT_ID,
      client_secret: process.env.GOOGLE_ADS_CLIENT_SECRET,
      developer_token: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
    });

    const customer = client.Customer({
      customer_id: process.env.BBT_CUSTOMER_ID,
      refresh_token: process.env.GOOGLE_ADS_REFRESH_TOKEN,
    });

    console.log("=== CUSTOMER OBJECT METHODS ===");
    console.log("Available methods:");
    const methods = Object.getOwnPropertyNames(
      Object.getPrototypeOf(customer)
    ).filter((name) => typeof customer[name] === "function");

    methods.forEach((method) => {
      console.log(`- ${method}`);
    });

    console.log("\n=== CUSTOMER OBJECT PROPERTIES ===");
    const props = Object.getOwnPropertyNames(customer);
    props.forEach((prop) => {
      console.log(`- ${prop}: ${typeof customer[prop]}`);
    });

    // Test if there are service-specific methods
    console.log("\n=== CHECKING SERVICE METHODS ===");
    if (customer.offlineUserDataJobs) {
      console.log(
        "offlineUserDataJobs exists:",
        typeof customer.offlineUserDataJobs
      );
    } else {
      console.log("offlineUserDataJobs does NOT exist");
    }

    // Check if mutate methods exist
    const mutateMethods = methods.filter(
      (m) => m.includes("mutate") || m.includes("Mutate")
    );
    console.log("\nMutate methods found:", mutateMethods);

    // Check if there are any offline data job related methods
    const offlineMethods = methods.filter((m) =>
      m.toLowerCase().includes("offline")
    );
    console.log("Offline methods found:", offlineMethods);

    return true;
  } catch (error) {
    console.error("Error:", error.message);
    return false;
  }
}

testApiMethods();
