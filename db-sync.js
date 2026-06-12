/**
 * ⚡ ESHU CODEX - STABILIZED CORE MUTEX SYNC ENGINE ⚡
 * Fixes: Mismatched KVDB keys & Thread Collisions across live domains.
 */

// Fixed and verified absolute path key
const CLOUD_DB_URL = "https://kvdb.io/6MfPVH9EQ4yuMB6HmwmG3/";
let isNetworkBusy = false;

/**
 * Mutex traffic lockdown logic to stop simultaneous fetch streams from dropping pipeline sockets
 */
async function executeSecureNetworkTask(taskFunction) {
    if (isNetworkBusy) {
        return null;
    }
    isNetworkBusy = true;
    try {
        return await taskFunction();
    } catch (error) {
        console.error("❌ Nexus Sync Engine Exception:", error);
        throw error;
    } finally {
        setTimeout(() => {
            isNetworkBusy = false;
        }, 300);
    }
}

/**
 * Pulls master configurations down from kvdb bucket into active application layout structures
 */
async function pullFromCloudRegistry() {
    return await executeSecureNetworkTask(async () => {
        // 1. Pull system config notifications
        try {
            const configRes = await fetch(`${CLOUD_DB_URL}global_system_config?nocache=${Date.now()}`);
            if (configRes.ok) {
                const configData = await configRes.json();
                if (configData && configData.global_admin_notice !== undefined) {
                    localStorage.setItem('global_admin_notice', configData.global_admin_notice);
                }
            }
        } catch (e) { console.warn("Notice sync skipped."); }

        // 2. Pull synchronized central CRM user registry list
        try {
            const usersRes = await fetch(`${CLOUD_DB_URL}admin_crm_users?nocache=${Date.now()}`);
            if (usersRes.ok) {
                const usersData = await usersRes.json();
                localStorage.setItem('admin_crm_users', JSON.stringify(usersData || []));
            }
        } catch (e) { console.warn("CRM Registry sync skipped."); }

        // 3. Pull transaction validation requests streams
        try {
            const queueRes = await fetch(`${CLOUD_DB_URL}admin_utr_queue?nocache=${Date.now()}`);
            if (queueRes.ok) {
                const queueData = await queueRes.json();
                localStorage.setItem('admin_utr_queue', JSON.stringify(queueData || []));
            }
        } catch (e) { console.warn("UTR Queue sync skipped."); }
    });
}

/**
 * Pushes active structural configuration layers up to remote database endpoints
 */
async function uploadToCloudRegistry() {
    return await executeSecureNetworkTask(async () => {
        const configPayload = {
            global_admin_notice: localStorage.getItem('global_admin_notice') || "",
            admin_crm_users: JSON.parse(localStorage.getItem('admin_crm_users') || '[]'),
            admin_utr_queue: JSON.parse(localStorage.getItem('admin_utr_queue') || '[]')
        };

        try {
            await fetch(`${CLOUD_DB_URL}global_system_config`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(configPayload)
            });
        } catch (e) { console.error("Cloud push failed:", e); }
    });
}

/**
 * Helper logic to transmit an individual user profile payload record
 */
async function pushUserProfileToCloud(uid, profileData) {
    try {
        await fetch(`${CLOUD_DB_URL}user_profile_${uid}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(profileData)
        });
    } catch (e) { console.error("Profile payload stream failed:", e); }
}

/**
 * Core function for user layout side to push new UTR entries straight into the global queue
 */
async function forceDirectCloudUtrPush(newUtrRecord) {
    try {
        // Dynamic fetch of current live queue state right before appending new model data
        const queueRes = await fetch(`${CLOUD_DB_URL}admin_utr_queue?nocache=${Date.now()}`);
        let currentLiveQueue = [];
        if (queueRes.ok) {
            const currentLiveQueueText = await queueRes.text();
            if (currentLiveQueueText && currentLiveQueueText.trim() !== "") {
                currentLiveQueue = JSON.parse(currentLiveQueueText);
            }
        }
        
        // Block duplicates immediately
        if (currentLiveQueue.some(item => item.utr === newUtrRecord.utr)) {
            alert("This transaction reference UTR has already been submitted.");
            return false;
        }

        currentLiveQueue.push(newUtrRecord);
        localStorage.setItem('admin_utr_queue', JSON.stringify(currentLiveQueue));

        const updateRes = await fetch(`${CLOUD_DB_URL}admin_utr_queue`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(currentLiveQueue)
        });

        return updateRes.ok;
    } catch (e) {
        console.error("UTR Pipeline submission failed:", e);
        return false;
    }
}
