/**
 * NEXUS AI NETWORK - Distributed Cloud Sync Routing Core
 * Safely aggregates client profile storage structures into globally available entities
 */

const CLOUD_CONFIG = {
    // Free JSON Storage API or Vercel API Endpoint mapping
    API_ENDPOINT: "https://api.jsonbin.io/v3/b/660d5b12e41b4d34e4de5e85", 
    MASTER_KEY: "$2b$10$SampleKeyReplaceWithYourActualSecretKeyIfUsingSecureBins"
};

/**
 * Pushes the current local database state into the shared network cloud
 */
async function uploadToCloudRegistry() {
    const backupPayload = {};
    // Extract user matrix indexes from active local scopes
    const usersList = JSON.parse(localStorage.getItem("admin_crm_users") || "[]");
    backupPayload["admin_crm_users"] = usersList;

    usersList.forEach(userId => {
        backupPayload[`pwd_${userId}`] = localStorage.getItem(`pwd_${userId}`);
        backupPayload[`balance_${userId}`] = localStorage.getItem(`balance_${userId}`);
        backupPayload[`revenue_${userId}`] = localStorage.getItem(`revenue_${userId}`);
        backupPayload[`packages_${userId}`] = localStorage.getItem(`packages_${userId}`);
    });

    try {
        await fetch(CLOUD_CONFIG.API_ENDPOINT, {
            method: "PUT",
            headers: {
                "Content-Type": "application/json",
                "X-Master-Key": CLOUD_CONFIG.MASTER_KEY
            },
            body: JSON.stringify(backupPayload)
        });
    } catch (error) {
        console.error("Network cloud propagation tracking delayed:", error);
    }
}

/**
 * Downloads updates from the cloud to reflect balances/coins modified by the admin
 */
async function pullFromCloudRegistry() {
    try {
        const response = await fetch(`${CLOUD_CONFIG.API_ENDPOINT}/latest`, {
            method: "GET",
            headers: { "X-Master-Key": CLOUD_CONFIG.MASTER_KEY }
        });
        const remoteData = await response.json();
        const recordData = remoteData.record || remoteData;

        // Force overwrite update mapping locally
        Object.keys(recordData).forEach(storageKey => {
            if (typeof recordData[storageKey] === 'object') {
                localStorage.setItem(storageKey, JSON.stringify(recordData[storageKey]));
            } else {
                localStorage.setItem(storageKey, recordData[storageKey]);
            }
        });
    } catch (error) {
        console.error("Cloud synchronization mapping check error:", error);
    }
}
