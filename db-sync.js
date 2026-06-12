// db-sync.js - Centralized Live Cloud Sync Routing Engine
const CLOUD_DB_URL = "https://kvdb.io/6MfPVH9EQ4yuMB6HmwmG3/"; 

/**
 * Pushes a new user UTR deposit ticket instantly into the cloud database with a retry mechanism.
 * This ensures any submission updates globally across all admin dashboards instantly.
 */
async function forceDirectCloudUtrPush(utrRecord) {
    const maxRetries = 3;
    let attempt = 0;

    while (attempt < maxRetries) {
        try {
            // Pull the latest global state directly from the server to avoid local overwrites
            const response = await fetch(`${CLOUD_DB_URL}admin_utr_queue?nocache=${Date.now()}`);
            let globalQueue = [];
            
            if (response.ok) {
                const rawText = await response.text();
                if (rawText && rawText.trim() !== "") {
                    globalQueue = JSON.parse(rawText);
                }
            }

            if (!Array.isArray(globalQueue)) globalQueue = [];

            // Check if this UTR has already been pushed by looking at the global data array
            if (globalQueue.some(item => item.utr === utrRecord.utr)) {
                console.log("Duplicate global UTR entry detected.");
                return true; 
            }

            // Set default pending status before broadcast distribution
            utrRecord.status = "pending";
            globalQueue.push(utrRecord);

            // POST the updated queue array back up to the master bucket
            const saveResponse = await fetch(`${CLOUD_DB_URL}admin_utr_queue`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(globalQueue)
            });

            if (!saveResponse.ok) throw new Error("Cloud rejected global payload database save");

            // Sync down to the individual device storage
            localStorage.setItem('admin_utr_queue', JSON.stringify(globalQueue));
            return true; 
            
        } catch (error) {
            attempt++;
            console.warn(`Database connection attempt ${attempt} failed:`, error);
            if (attempt >= maxRetries) {
                console.error("Master transmission network link fault:", error);
                return false; 
            }
            // Brief timeout pause before retrying the server stream connection
            await new Promise(resolve => setTimeout(resolve, 500));
        }
    }
    return false;
}

/**
 * Downloads centralized live database registries to sync the current device state.
 */
async function pullFromCloudRegistry() {
    try {
        const resUtr = await fetch(`${CLOUD_DB_URL}admin_utr_queue?nocache=${Date.now()}`);
        if (resUtr.ok) {
            const txtUtr = await resUtr.text();
            localStorage.setItem('admin_utr_queue', (txtUtr && txtUtr.trim() !== "") ? txtUtr : '[]');
        }

        const resUsers = await fetch(`${CLOUD_DB_URL}admin_crm_users?nocache=${Date.now()}`);
        if (resUsers.ok) {
            const txtUsers = await resUsers.text();
            localStorage.setItem('admin_crm_users', (txtUsers && txtUsers.trim() !== "") ? txtUsers : '[]');
        }

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
        console.error("Global system configuration sync pull dropped:", e);
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
        console.error("Global system push execution error:", e);
        return false;
    }
}
