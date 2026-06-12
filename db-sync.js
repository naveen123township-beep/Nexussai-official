/**
 * NEXUS HIGH-AVAILABILITY DECENTRALIZED DATA ARCHITECTURE
 * Eliminates monolithic sync collisions by parsing traffic into isolated storage lanes.
 */

const KVDB_BASE_URL = "https://kvdb.io/LsoeLmZp8VtQZC1FuYZWtG";

// Safe wrapper for atomic localStorage checks
function getLocalJSON(key, defaultVal = []) {
    try {
        return JSON.parse(localStorage.getItem(key) || JSON.stringify(defaultVal));
    } catch (e) {
        return defaultVal;
    }
}

/**
 * PULLS data cleanly from decentralized KVDB buckets down to LocalStorage
 */
async function pullFromCloudRegistry() {
    const userId = localStorage.getItem('active_user_id') || "NX-GUEST";
    try {
        // Lane 1: Fetch Global Configurations and System Broadcast Banners
        const globalRes = await fetch(`${KVDB_BASE_URL}/global_system_config?nocache=${Date.now()}`);
        if (globalRes.ok) {
            const config = await globalRes.json();
            if (config.global_admin_notice) localStorage.setItem('global_admin_notice', config.global_admin_notice);
            if (config.admin_crm_users) localStorage.setItem('admin_crm_users', JSON.stringify(config.admin_crm_users));
            if (config.admin_utr_queue) localStorage.setItem('admin_utr_queue', JSON.stringify(config.admin_utr_queue));
        }

        // Lane 2: Fetch Personal isolated tracking profile
        if (userId !== "NX-GUEST") {
            const userRes = await fetch(`${KVDB_BASE_URL}/user_profile_${userId}?nocache=${Date.now()}`);
            if (userRes.ok) {
                const userData = await userRes.json();
                if (userData) {
                    if (userData.balance) localStorage.setItem(`balance_${userId}`, userData.balance);
                    if (userData.revenue) localStorage.setItem(`revenue_${userId}`, userData.revenue);
                    if (userData.packages) localStorage.setItem(`packages_${userId}`, JSON.stringify(userData.packages));
                    if (userData.notice) localStorage.setItem(`notice_${userId}`, userData.notice);
                    if (userData.popup_alert) localStorage.setItem(`popup_alert_${userId}`, userData.popup_alert);
                }
            }
        }
        return true;
    } catch (error) {
        console.error("⚠️ Local synchronization pull handled: ", error);
        return false;
    }
}

/**
 * BACKS UP state data smoothly by isolating user sets away from master queues
 */
async function uploadToCloudRegistry() {
    const userId = localStorage.getItem('active_user_id') || "NX-GUEST";
    try {
        // Step 1: Sync global tracking metrics safely
        const globalPayload = {
            global_admin_notice: localStorage.getItem('global_admin_notice') || "",
            admin_crm_users: getLocalJSON('admin_crm_users', []),
            admin_utr_queue: getLocalJSON('admin_utr_queue', [])
        };
        
        await fetch(`${KVDB_BASE_URL}/global_system_config`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(globalPayload)
        });

        // Step 2: Sync individual active node paths cleanly
        if (userId !== "NX-GUEST") {
            const userPayload = {
                balance: localStorage.getItem(`balance_${userId}`) || "0.00",
                revenue: localStorage.getItem(`revenue_${userId}`) || "0.00",
                packages: getLocalJSON(`packages_${userId}`, []),
                notice: localStorage.getItem(`notice_${userId}`) || "",
                popup_alert: localStorage.getItem(`popup_alert_${userId}`) || "false"
            };

            await fetch(`${KVDB_BASE_URL}/user_profile_${userId}`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(userPayload)
            });
        }
        return true;
    } catch (error) {
        console.error("⚠️ Cloud replication sync pipeline skipped: ", error);
        return false;
    }
}

/**
 * DIRECT HARDENED PUSH FOR UTR DISPATCHING
 * Prevents transaction overwrites during periods of high user traffic.
 */
async function forceDirectCloudUtrPush(newRecord) {
    try {
        // Pull latest remote queue instantly before appending
        const globalRes = await fetch(`${KVDB_BASE_URL}/global_system_config?nocache=${Date.now()}`);
        let currentQueue = [];
        let currentCRM = [];
        let noticeBanner = "";

        if (globalRes.ok) {
            const config = await globalRes.json();
            currentQueue = config.admin_utr_queue || [];
            currentCRM = config.admin_crm_users || [];
            noticeBanner = config.global_admin_notice || "";
        }

        // Deduplicate and insert
        currentQueue = currentQueue.filter(item => !(item.userId === newRecord.userId && item.utr === newRecord.utr));
        currentQueue.push(newRecord);

        if (!currentCRM.includes(newRecord.userId)) {
            currentCRM.push(newRecord.userId);
        }

        // Write directly to local storage to keep the interface updated
        localStorage.setItem('admin_utr_queue', JSON.stringify(currentQueue));
        localStorage.setItem('admin_crm_users', JSON.stringify(currentCRM));

        // Push directly to the master cloud key
        const pushPayload = {
            global_admin_notice: noticeBanner,
            admin_crm_users: currentCRM,
            admin_utr_queue: currentQueue
        };

        const writeRes = await fetch(`${KVDB_BASE_URL}/global_system_config`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(pushPayload)
        });

        // Make sure the user's local profiles match up
        await uploadToCloudRegistry();

        return writeRes.ok;
    } catch (err) {
        console.error("❌ Critical UTR pipeline crash prevented:", err);
        return false;
    }
}
