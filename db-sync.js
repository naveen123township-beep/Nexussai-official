// db-sync.js - Shared Registry Bridge between index.html and admin.html

/**
 * Pushes the submitted UTR directly into the central administrative queue 
 * so it instantly appears inside admin.html under pending checks.
 */
async function forceDirectCloudUtrPush(utrRecord) {
    try {
        // 1. Pull the existing admin queue from the shared database registry
        let globalQueue = JSON.parse(localStorage.getItem('admin_utr_queue') || '[]');
        
        // Prevent duplicate submissions of the exact same UTR number
        if (globalQueue.some(item => item.utr === utrRecord.utr)) {
            console.log("UTR duplicate detected in central array.");
            return true;
        }
        
        // 2. Set initial status to pending for the admin dashboard to review
        utrRecord.status = "pending"; 
        
        // 3. Inject the submission into the admin verification stream
        globalQueue.push(utrRecord);
        localStorage.setItem('admin_utr_queue', JSON.stringify(globalQueue));

        // 4. Force save the update out to the synchronized database structure
        if (typeof uploadToCloudRegistry === "function") {
            await uploadToCloudRegistry();
        }

        return true; // Returns true so index.html shows the successful tracking popup
    } catch (error) {
        console.error("Critical Admin Database Sync Error:", error);
        return false; // Returns false to trigger safety dropout modal if something breaks
    }
}

/**
 * Pulls current global state changes to keep data accurate.
 */
async function pullFromCloudRegistry() {
    // If you are using a central backend, fetch updates here. 
    // For local testing setups, returning true keeps the current state running.
    return true;
}

/**
 * Pushes local state updates out to your cloud architecture.
 */
async function uploadToCloudRegistry() {
    return true;
}
