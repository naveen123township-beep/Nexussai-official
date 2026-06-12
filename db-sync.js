/**
 * ESHU CODEX - High-Availability Segregated Node Sync Engine
 */

const BASE_CLUSTER_URL = "https://kvdb.io/LsoeLmZp8VtQZC1FuYZWtG/";

let lastSyncedPayloadString = "";
let isSyncInProgress = false;

function getActiveContextKey() {
    return "global_nexus_master_registry";
}

async function forceDirectCloudUtrPush(newUtrRecord) {
    const contextKey = getActiveContextKey();
    
    try {
        // Pull latest remote data state directly using clean headers
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

        // Map collection layout to prevent incoming payloads from overwriting existing database keys
        const unifiedMap = new Map();
        serverQueue.forEach(item => unifiedMap.set(`${item.userId}_${item.utr}`, item));
        
        // Add or update the current entry 
        unifiedMap.set(`${newUtrRecord.userId}_${newUtrRecord.utr}`, newUtrRecord);
        const finalMergedQueue = Array.from(unifiedMap.values());

        const mergedUsersSet = new Set(serverUsers);
        mergedUsersSet.add(newUtrRecord.userId);
        const finalMergedUsers = Array.from(mergedUsersSet);

        serverData.admin_utr_queue = finalMergedQueue;
        serverData.admin_crm_users = finalMergedUsers;

        // Push directly to cloud storage node
        const pushRes = await fetch(`${BASE_CLUSTER_URL}${contextKey}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(serverData)
        });

        if (pushRes.ok) {
            localStorage.setItem('admin_utr_queue', JSON.stringify(finalMergedQueue));
            localStorage.setItem('admin_crm_users', JSON.stringify(finalMergedUsers));
            lastSyncedPayloadString = JSON.stringify(serverData);
            return true;
        }
        return false;
    } catch (err) {
        console.error("Cloud pipeline drop logged:", err);
        return false;
    }
}
