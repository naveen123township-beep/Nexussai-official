/**
 * NEXUS HIGH-AVAILABILITY CLOUD DATABASE BRIDGE
 * Works exactly like a Telegram bot registry—reads and writes straight to the cloud.
 */

const KVDB_BASE_URL = "https://kvdb.io/LsoeLmZp8VtQZC1FuYZWtG";

function getLocalJSON(key, defaultVal = []) {
    try {
        return JSON.parse(localStorage.getItem(key) || JSON.stringify(defaultVal));
    } catch (e) {
        return defaultVal;
    }
}

/**
 * Sync data cleanly from the KVDB bucket down to the local state
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

        // Lane 2: Fetch Personal isolated user account profile
        if (userId !== "NX-GUEST") {
            const userRes = await fetch(`${KVDB_BASE_URL}/user_profile_${userId}?nocache=${Date.now()}`);
            if (userRes.ok) {
                const userData = await userRes.json();
                if (userData) {
                    localStorage.setItem(`balance_${userId}`, userData.balance || "0.00");
                    localStorage.setItem(`revenue_${userId}`, userData.revenue || "0.00");
                    localStorage.setItem(`packages_${userId}`, JSON.stringify(userData.packages || []));
                    localStorage.setItem(`notice_${userId}`, userData.notice || "");
                    localStorage.setItem(`popup_alert_${userId}`, userData.popup_alert || "false");
                }
            }
        }
        return true;
    } catch (error) {
        console.error("Cloud synchronization pull exception handled: ", error);
        return false;
    }
}

/**
 * Pushes data safely to the cloud cluster
 */
async function uploadToCloudRegistry() {
    const userId = localStorage.getItem('active_user_id') || "NX-GUEST";
    try {
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
        console.error("Cloud replication sync pipeline exception skipped: ", error);
        return false;
    }
}

/**
 * Direct pipeline push to register new user transactions securely
 */
async function forceDirectCloudUtrPush(newRecord) {
    try {
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

        currentQueue = currentQueue.filter(item => !(item.userId === newRecord.userId && item.utr === newRecord.utr));
        currentQueue.push(newRecord);

        if (!currentCRM.includes(newRecord.userId)) {
            currentCRM.push(newRecord.userId);
        }

        localStorage.setItem('admin_utr_queue', JSON.stringify(currentQueue));
        localStorage.setItem('admin_crm_users', JSON.stringify(currentCRM));

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

        await uploadToCloudRegistry();
        return writeRes.ok;
    } catch (err) {
        console.error("UTR tracking pipeline exception prevented:", err);
        return false;
    }
}
