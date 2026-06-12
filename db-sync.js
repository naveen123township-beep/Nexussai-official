/**
 * ESHU CODEX - High-Availability Segregated Node Sync Engine
 * Fix: Removed URL parameters to prevent KVDB network dropouts.
 */

const BASE_CLUSTER_URL = "https://kvdb.io/LsoeLmZp8VtQZC1FuYZWtG/";

let lastSyncedPayloadString = "";
let isSyncInProgress = false;

function getActiveContextKey() {
    return "global_nexus_master_registry";
}

async function pullFromCloudRegistry() {
    if (isSyncInProgress) return false;
    const contextKey = getActiveContextKey();
    
    try {
        const response = await fetch(`${BASE_CLUSTER_URL}${contextKey}`, {
            method: "GET",
            headers: { 
                "Accept": "application/json",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache"
            }
        });

        if (response.status === 404) return true;
        if (!response.ok) throw new Error(`Status ${response.status}`);

        const remoteData = await response.json();
        if (!remoteData || typeof remoteData !== "object") return false;

        Object.keys(remoteData).forEach(key => {
            let val = remoteData[key];
            if (typeof val === "object") val = JSON.stringify(val);
            localStorage.setItem(key, val);
        });

        lastSyncedPayloadString = JSON.stringify(remoteData);
        return true;
    } catch (error) {
        console.warn("⚠️ Downstream sync bypassed:", error.message);
        return false;
    }
}

async function uploadToCloudRegistry() {
    if (isSyncInProgress) return false;
    const contextKey = getActiveContextKey();
    
    try {
        const payload = {};

        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            
            if (key.startsWith("NX-") || key.startsWith("pwd_") || key.startsWith("balance_") || 
                key.startsWith("revenue_") || key.startsWith("packages_") || key.startsWith("notice_") || 
                key.startsWith("popup_alert_") || key.includes("admin_crm_users") || 
                key.includes("admin_utr_queue") || key.includes("global_admin_notice")) {
                
                let val = localStorage.getItem(key);
                if (val && (val.startsWith("[") || val.startsWith("{"))) {
                    try { val = JSON.parse(val); } catch(e) {}
                }
                payload[key] = val;
            }
        }

        const currentPayloadString = JSON.stringify(payload);
        if (currentPayloadString === lastSyncedPayloadString || currentPayloadString === "{}") return true;

        isSyncInProgress = true;
        
        const response = await fetch(`${BASE_CLUSTER_URL}${contextKey}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: currentPayloadString
        });

        isSyncInProgress = false;
        if (!response.ok) throw new Error(`Upstream replication denied: ${response.status}`);

        lastSyncedPayloadString = currentPayloadString;
        return true;
    } catch (error) {
        isSyncInProgress = false;
        console.error("❌ Upload aborted:", error.message);
        return false;
    }
}

/**
 * FIXED ATOMIC PIPELINE: Clean headers fix to ensure UTR reaches the admin panel phone safely.
 */
async function forceDirectCloudUtrPush(newUtrRecord) {
    const contextKey = getActiveContextKey();
    try {
        // Fetch current snapshot without altering the URL path structure
        const res = await fetch(`${BASE_CLUSTER_URL}${contextKey}`, {
            method: "GET",
            headers: {
                "Cache-Control": "no-cache",
                "Pragma": "no-cache"
            }
        });
        
        let serverData = {};
        if (res.ok) {
            serverData = await res.json();
        }

        let serverQueue = [];
        if (serverData.admin_utr_queue) {
            serverQueue = typeof serverData.admin_utr_queue === 'string' 
                ? JSON.parse(serverData.admin_utr_queue) 
                : serverData.admin_utr_queue;
        }
        
        let serverUsers = [];
        if (serverData.admin_crm_users) {
            serverUsers = typeof serverData.admin_crm_users === 'string' 
                ? JSON.parse(serverData.admin_crm_users) 
                : serverData.admin_crm_users;
        }

        // Clean out duplicates
        serverQueue = serverQueue.filter(item => !(item.userId === newUtrRecord.userId && item.utr === newUtrRecord.utr));
        
        // Append the new record entry
        serverQueue.push(newUtrRecord);
        
        if (!serverUsers.includes(newUtrRecord.userId)) {
            serverUsers.push(newUtrRecord.userId);
        }

        serverData.admin_utr_queue = serverQueue;
        serverData.admin_crm_users = serverUsers;

        // Sync directly to the cloud cluster
        const pushRes = await fetch(`${BASE_CLUSTER_URL}${contextKey}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(serverData)
        });

        if (pushRes.ok) {
            localStorage.setItem('admin_utr_queue', JSON.stringify(serverQueue));
            localStorage.setItem('admin_crm_users', JSON.stringify(serverUsers));
            lastSyncedPayloadString = JSON.stringify(serverData);
            return true;
        }
        return false;
    } catch (err) {
        console.error("Critical Cloud Pipeline Drop:", err);
        return false;
    }
}

if (typeof window !== 'undefined') {
    window.pullFromCloudRegistry = pullFromCloudRegistry;
    window.uploadToCloudRegistry = uploadToCloudRegistry;
    window.forceDirectCloudUtrPush = forceDirectCloudUtrPush;
}
