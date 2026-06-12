/**
 * ESHU CODEX - High-Availability Segregated Node Sync Engine
 * Fix: Removed race-condition context overrides to ensure flawless multi-device synchronization.
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
        const response = await fetch(`${BASE_CLUSTER_URL}${contextKey}?nocache=${Date.now()}`, {
            method: "GET",
            headers: { "Accept": "application/json" }
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
 * FIXED ATOMIC PIPELINE: Eliminates dropped packets when submitting a UTR reference
 */
async function forceDirectCloudUtrPush(newUtrRecord) {
    const contextKey = getActiveContextKey();
    try {
        // Step 1: Grab the absolutely latest server state down first
        const res = await fetch(`${BASE_CLUSTER_URL}${contextKey}?nocache=${Date.now()}`);
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

        // Drop historical matches if resubmitting rejected nodes
        serverQueue = serverQueue.filter(item => !(item.userId === newUtrRecord.userId && item.utr === newUtrRecord.utr));
        
        // Push newly formatted node tracking array block
        serverQueue.push(newUtrRecord);
        
        if (!serverUsers.includes(newUtrRecord.userId)) {
            serverUsers.push(newUtrRecord.userId);
        }

        // Re-assign serialized arrays cleanly to avoid storage faults
        serverData.admin_utr_queue = serverQueue;
        serverData.admin_crm_users = serverUsers;

        // Step 2: Push complete bundle straight back up to clear the sync pipeline
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
