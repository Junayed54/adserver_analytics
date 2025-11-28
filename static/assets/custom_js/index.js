document.addEventListener('DOMContentLoaded', () => {
    // Define the URL for your sign-in page
    const SIGN_IN_PAGE_URL = '/sign-in/'; 
    // You can adjust 'sign-in.html' if your file is named differently (e.g., 'login.html')

    // ============================
    // 1. Select DOM Elements
    // ============================
    const loginBtn = document.getElementById('login-btn');
    const signupBtn = document.getElementById('signup-btn');
    const logoutBtn = document.getElementById('logout-btn');

    // ============================
    // 2. Check Local Storage
    // ============================
    const token = localStorage.getItem('access_token');

    // ============================
    // 3. Authorization Check and UI Update
    // ============================
    if (token) {
        // --- User IS Logged In ---
        console.log('Access token found. Showing content and logout button.');

        // Hide the guest buttons
        if(loginBtn) loginBtn.style.display = 'none';
        if(signupBtn) signupBtn.style.display = 'none';

        // Show the logout button
        if(logoutBtn) logoutBtn.style.display = 'inline-block';

    } else {
        // --- User is NOT Logged In ---
        console.log('No access token found. Checking redirection status.');

        // IMPORTANT: Prevent infinite loop by checking the current URL.
        // If the token is missing AND the user is NOT already on the sign-in page, redirect them.
        const currentPage = window.location.pathname;

        if (!currentPage.includes(SIGN_IN_PAGE_URL)) {
            console.log(`Redirecting to ${SIGN_IN_PAGE_URL}`);
            window.location.href = SIGN_IN_PAGE_URL;
        } else {
            // If they are on the sign-in page, just ensure the UI is correct for a guest
            if(loginBtn) loginBtn.style.display = 'inline-block';
            if(signupBtn) signupBtn.style.display = 'inline-block';
            if(logoutBtn) logoutBtn.style.display = 'none';
        }
    }


    // ============================
    // 4. Handle Logout (Always needed)
    // ============================
    if (logoutBtn) {
        logoutBtn.addEventListener('click', () => {
            console.log('Logout clicked. Removing token...');
            
            // Remove the token
            localStorage.removeItem('access_token');

            // Redirect the user to the sign-in page immediately after logging out
            window.location.href = SIGN_IN_PAGE_URL;
        });
    }
});