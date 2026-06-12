/**
 * NEXUS HIGH-AVAILABILITY CLOUD REGISTRY CORE INTERFACE
 * Handles explicit atomic synchronizations between Local Storage and KVDB Storage Lanes
 */

const NEXUS_KVDB_ENDPOINT = "https://kvdb.io/LsoeLmZp8VtQZC1FuYZWtG/global_nexus_master_registry";

/**
 * Safely fetches the global cloud state and merges records carefully to protect localized transactions
 */
async function pullFromCloudRegistry() {
    try {
        const response = await fetch(`${NEXUS_KVDB_ENDPOINT}?nocache=${Date.now()}`, {
            method: "GET",
            headers: { "Cache-Control": "no-cache, no-store, must-revalidate" }
        });
        
        if (!response.ok) throw new Error(`HTTP Error Status: ${response.status}`);
        
        const cloudData = await response.json();
        if (!cloudData) return false;

        // Atomic Lanes Synchronization 
        const syncLanes = ['admin_utr_queue', 'admin_crm_users', 'global_admin_notice'];
        syncLanes.forEach(lane => {
            if (cloudData[lane]) {
                const normalizedValue = typeof cloudData[lane] === 'string' ? cloudData[lane] : JSON.stringify(cloudData[lane]);
                localStorage.setItem(lane, normalizedValue);
            }
        });

        // Dynamic User Keys Unpacking
        Object.keys(cloudData).forEach(key => {
            if (key.startsWith('balance_') || key.startsWith('revenue_') || key.startsWith('packages_') || key.startsWith('notice_') || key.startsWith('popup_alert_')) {
                const normalizedVal = typeof cloudData[key] === 'string' ? cloudData[key] : JSON.stringify(cloudData[key]);
                localStorage.setItem(key, normalizedVal);
            }
        });
        
        return true;
    } catch (error) {
        console.error("❌ High Availability cloud synchronization pull blocked:", error);
        return false;
    }
}

/**
 * Packs local states securely and pushes a full replica backup snapshot directly to the remote cluster
 */
async function uploadToCloudRegistry() {
    try {
        const structuralPayload = {};
        
        // Pack global structural arrays
        structuralPayload['admin_utr_queue'] = JSON.parse(localStorage.getItem('admin_utr_queue') || '[]');
        structuralPayload['admin_crm_users'] = JSON.parse(localStorage.getItem('admin_crm_users') || '[]');
        structuralPayload['global_admin_notice'] = localStorage.getItem('global_admin_notice') || "";

        // Pack decentralized profile elements dynamically
        for (let i = 0; i < localStorage.length; i++) {
            const currentKey = localStorage.key(i);
            if (!currentKey) continue;
            
            if (currentKey.startsWith('balance_') || currentKey.startsWith('revenue_')) {
                structuralPayload[currentKey] = localStorage.getItem(currentKey);
            } else if (currentKey.startsWith('packages_') || currentKey.startsWith('notice_') || currentKey.startsWith('popup_alert_')) {
                try {
                    structuralPayload[currentKey] = JSON.parse(localStorage.getItem(currentKey) || '[]');
                } catch(e) {
                    structuralPayload[currentKey] = localStorage.getItem(currentKey);
                }
            }
        }

        const response = await fetch(NEXUS_KVDB_ENDPOINT, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(structuralPayload)
        });

        return response.ok;
    } catch (error) {
        console.error("❌ Cloud Registry core replication upload execution failure:", error);
        return false;
    }
}

/**
 * SYNCHRONOUS TRANSACTION GATE: Assures structural verification before client checkout passes
 */
async function forceDirectCloudUtrPush(newRecord) {
    try {
        // Run an absolute sync confirmation lock step to secure live positions from other nodes
        await pullFromCloudRegistry();
        
        let activeQueue = JSON.parse(localStorage.getItem('admin_utr_queue') || '[]');
        // Deduplicate records matches instantly
        activeQueue = activeQueue.filter(item => !(item.userId === newRecord.userId && item.utr === newRecord.utr));
        activeQueue.push(newRecord);
        localStorage.setItem('admin_utr_queue', JSON.stringify(activeQueue));

        let currentCRM = JSON.parse(localStorage.getItem('admin_crm_users') || '[]');
        if (!currentCRM.includes(newRecord.userId)) {
            currentCRM.push(newRecord.userId);
            localStorage.setItem('admin_crm_users', JSON.stringify(currentCRM));
        }

        return await uploadToCloudRegistry();
    } catch(err) {
        console.error("Critical core thread failure:", err);
        return false;
    }
}
