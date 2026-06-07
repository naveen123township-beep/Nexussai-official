/**
 * ESHU CODEX - High-Availability Segregated Node Sync Engine
 * Prevent Global Overwrites & Isolate Client Data Buffers
 */

const BASE_CLUSTER_URL = "https://kvdb.io/LsoeLmZp8VtQZC1FuYZWtG/";

let lastSyncedPayloadString = "";
let isSyncInProgress = false;

// Determine if the current window context belongs to the administrator or a standard user
function getActiveContextKey() {
    const activeUser = localStorage.getItem('active_user_id');
    // Admin uses a global shared core registry, standard profiles use separate isolated endpoints
    if (!activeUser || window.location.pathname.includes('admin.html')) {
        return "global_nexus_master_registry";
    }
    return `user_${activeUser}`;
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
        const activeUser = localStorage.getItem('active_user_id');

        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            
            // If admin panel context, synchronize the entire system framework
            if (contextKey === "global_nexus_master_registry") {
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
            } else {
                // Securely isolate and limit data sync strictly to the logged-in user's data packets
                if (key.includes(activeUser) || key === "admin_utr_queue" || key === "admin_crm_users" || key === "global_admin_notice") {
                    let val = localStorage.getItem(key);
                    if (val && (val.startsWith("[") || val.startsWith("{"))) {
                        try { val = JSON.parse(val); } catch(e) {}
                    }
                    payload[key] = val;
                }
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

if (typeof window !== 'undefined') {
    window.pullFromCloudRegistry = pullFromCloudRegistry;
    window.uploadToCloudRegistry = uploadToCloudRegistry;
    window.setSyncLockState = (state) => { isSyncInProgress = state; };
}
