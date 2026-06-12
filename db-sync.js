// db-sync.js - Live Centralized Cloud Storage Routing Engine

const CLOUD_DB_URL = "https://kvdb.io/6MfPVH9EQ4yuMB6HmwmG3/"; 

/**
 * Pushes a new user UTR deposit ticket instantly into the cloud database.
 */
async function forceDirectCloudUtrPush(utrRecord) {
    try {
        const response = await fetch(`${CLOUD_DB_URL}admin_utr_queue`);
        let globalQueue = [];
        
        if (response.ok) {
            const rawText = await response.text();
            if (rawText && rawText.trim() !== "") {
                globalQueue = JSON.parse(rawText);
            }
        }

        if (!Array.isArray(globalQueue)) globalQueue = [];

        if (globalQueue.some(item => item.utr === utrRecord.utr)) {
            console.log("Duplicate UTR sequence caught.");
            return true;
        }

        utrRecord.status = "pending";
        globalQueue.push(utrRecord);

        const saveResponse = await fetch(`${CLOUD_DB_URL}admin_utr_queue`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(globalQueue)
        });

        if (!saveResponse.ok) throw new Error("Cloud save rejection");

        localStorage.setItem('admin_utr_queue', JSON.stringify(globalQueue));
        return true;
    } catch (error) {
        console.error("Cloud Write Fault:", error);
        return false;
    }
}

/**
 * Downloads centralized live database registries to sync the current device state.
 */
async function pullFromCloudRegistry() {
    try {
        // 1. Fetch live UTR tickets
        const resUtr = await fetch(`${CLOUD_DB_URL}admin_utr_queue?nocache=${Date.now()}`);
        if (resUtr.ok) {
            const txtUtr = await resUtr.text();
            localStorage.setItem('admin_utr_queue', (txtUtr && txtUtr.trim() !== "") ? txtUtr : '[]');
        }

        // 2. Fetch live user registration logs
        const resUsers = await fetch(`${CLOUD_DB_URL}admin_crm_users?nocache=${Date.now()}`);
        if (resUsers.ok) {
            const txtUsers = await resUsers.text();
            localStorage.setItem('admin_crm_users', (txtUsers && txtUsers.trim() !== "") ? txtUsers : '[]');
        }

        // 3. Fetch global system configuration (Broadcast text notice alerts)
        const resConfig = await fetch(`${CLOUD_DB_URL}global_system_config?nocache=${Date.now()}`);
        if (resConfig.ok) {
            const txtConfig = await resConfig.text();
            if (txtConfig && txtConfig.trim() !== "") {
                const config = JSON.parse(txtConfig);
                localStorage.setItem('global_admin_notice', config.global_admin_notice || "");
            }
        }
        return true;
    } catch (e) {
        console.error("Cloud synchronization pull failed:", e);
        return false;
    }
}

/**
 * Uploads local ledger state changes up to the shared cloud database workspace.
 */
async function uploadToCloudRegistry() {
    try {
        const localUtr = localStorage.getItem('admin_utr_queue') || '[]';
        const localUsers = localStorage.getItem('admin_crm_users') || '[]';
        const notice = localStorage.getItem('global_admin_notice') || "";

        await fetch(`${CLOUD_DB_URL}admin_utr_queue`, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' },
            body: localUtr 
        });
        
        await fetch(`${CLOUD_DB_URL}admin_crm_users`, { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' },
            body: localUsers 
        });

        await fetch(`${CLOUD_DB_URL}global_system_config`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                global_admin_notice: notice,
                admin_crm_users: JSON.parse(localUsers),
                admin_utr_queue: JSON.parse(localUtr)
            })
        });
        return true;
    } catch (e) {
        console.error("Cloud synchronization push failed:", e);
        return false;
    }
}
