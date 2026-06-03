/**
 * ESHU CODEX - Realtime Cloud Sync Engine
 * Synchronizes local browser storage across multi-device sessions instantly.
 */

// Centralized KVdb configuration using your generated bucket
const BUCKET_ID = "LsoeLmZp8VtQZC1FuYZWtG"; 
const REMOTE_CLUSTER_URL = `https://kvdb.io/${BUCKET_ID}/nexus_database_registry`;

// Track baseline snapshots to manage network changes smoothly
let lastSyncedPayloadString = "";
let isSyncInProgress = false;

/**
 * Pulls the entire global database state from the cloud registry 
 * and maps variables into the local device storage context.
 */
async function pullFromCloudRegistry() {
    if (isSyncInProgress) return false;
    try {
        const response = await fetch(REMOTE_CLUSTER_URL, {
            method: "GET",
            headers: { "Accept": "application/json" }
        });

        // If bucket data is empty/newly generated, keep local data intact
        if (response.status === 404) {
            console.log("⚡ [ESHU CODEX]: Cloud registry empty. Initializing custom bucket keys...");
            return true;
        }

        if (!response.ok) throw new Error("Cloud sync engine rejected read frame.");

        const globalRegistry = await response.json();
        if (!globalRegistry || typeof globalRegistry !== "object") return false;

        // Sync keys securely straight into client device instance
        Object.keys(globalRegistry).forEach(storageKey => {
            let cloudValue = globalRegistry[storageKey];
            if (typeof cloudValue === "object") {
                cloudValue = JSON.stringify(cloudValue);
            }
            localStorage.setItem(storageKey, cloudValue);
        });

        lastSyncedPayloadString = JSON.stringify(globalRegistry);
        console.log("⚡ [ESHU CODEX]: Multi-device cloud state synced downward successfully.");
        return true;
    } catch (error) {
        console.error("❌ [SYNC ERROR]: Download pipeline dropped:", error.message);
        return false;
    }
}

/**
 * Packs all internal user assets, wallet logs, and pending UTR queues
 * and pushes the compiled structural registry up to the global cloud.
 */
async function uploadToCloudRegistry() {
    if (isSyncInProgress) return false;
    try {
        const localRegistryDataset = {};
        
        // Loop and isolate framework specific variables to keep transaction loads light
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            
            if (
                key.startsWith("NX-") ||
                key.startsWith("pwd_") ||
                key.startsWith("balance_") ||
                key.startsWith("revenue_") ||
                key.startsWith("packages_") ||
                key.startsWith("notice_") ||
                key.startsWith("popup_alert_") ||
                key === "admin_crm_users" ||
                key === "admin_utr_queue" ||
                key === "global_admin_notice"
            ) {
                let rawValue = localStorage.getItem(key);
                // Parse layout back to JSON arrays/objects cleanly before transmitting
                if (rawValue && (rawValue.startsWith("[") || rawValue.startsWith("{"))) {
                    try { rawValue = JSON.parse(rawValue); } catch(e) {}
                }
                localRegistryDataset[key] = rawValue;
            }
        }

        const currentPayloadString = JSON.stringify(localRegistryDataset);
        
        // Prevent wasting bandwidth if data hasn't changed since last tick
        if (currentPayloadString === lastSyncedPayloadString || currentPayloadString === "{}") {
            return true;
        }

        isSyncInProgress = true;
        const response = await fetch(REMOTE_CLUSTER_URL, {
            method: "POST", // KVdb accepts raw JSON payloads securely over standard POST paths
            headers: { "Content-Type": "application/json" },
            body: currentPayloadString
        });

        isSyncInProgress = false;
        if (!response.ok) throw new Error("Upstream server cluster rejected storage payload.");

        lastSyncedPayloadString = currentPayloadString;
        console.log("⚡ [ESHU CODEX]: Shared database state successfully committed upstream.");
        return true;
    } catch (error) {
        isSyncInProgress = false;
        console.error("❌ [SYNC ERROR]: Upload execution failure:", error.message);
        return false;
    }
}

// Global hook listener setup to capture data updates out-of-the-box
if (typeof window !== 'undefined') {
    window.pullFromCloudRegistry = pullFromCloudRegistry;
    window.uploadToCloudRegistry = uploadToCloudRegistry;
}
