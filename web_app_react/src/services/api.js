// src/services/api.js
import axios from 'axios';

// Use environment variable or hardcode for development
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api';

const apiClient = axios.create({
    baseURL: API_BASE_URL,
    withCredentials: true, // Send cookies (like session ID) with requests
});

// --- Auth ---
export const checkAuth = () => apiClient.get('/check_auth');
export const login = (username, password) => apiClient.post('/login', { username, password });
export const logout = () => apiClient.post('/logout');

// --- Tables ---
export const listTables = () => apiClient.get('/tables');
export const createTable = (tableName, columns) => apiClient.post('/tables', { table_name: tableName, columns: columns });
export const dropTable = (tableName) => apiClient.delete(`/tables/${tableName}`);
export const queryTable = (tableName, filter) => apiClient.post(`/tables/${tableName}/query`, { filter }); // Be cautious with filter!

// --- Views --- START NEW ---
export const listViews = () => apiClient.get('/views');
export const queryView = (viewName) => apiClient.post(`/views/${viewName}/query`);
// Add create/drop view functions if needed later
// --- Views --- END NEW ---

// --- Materialized Views --- START NEW ---
export const listMaterializedViews = () => apiClient.get('/materialized_views');
export const queryMaterializedView = (viewName) => apiClient.post(`/materialized_views/${viewName}/query`);
// Add create/drop/refresh materialized view functions if needed later
// --- Materialized Views --- END NEW ---

// --- Records ---
export const insertRecord = (tableName, record) => apiClient.post(`/tables/${tableName}/records`, { record });
// Add updateRecord, deleteRecord similarly...
export const updateRecord = (tableName, recordId, updates) => apiClient.put(`/tables/${tableName}/records/${recordId}`, { updates });
export const deleteRecord = (tableName, recordId) => apiClient.delete(`/tables/${tableName}/records/${recordId}`);


// --- Procedures ---
export const createProcedure = (procedureName, procedureCode) => apiClient.post('/procedures', { procedure_name: procedureName, procedure_code: procedureCode }); // Security risk!
export const executeProcedure = (procedureName, params) => apiClient.post(`/procedures/${procedureName}/execute`, { params });

// --- DB Info ---
export const getDbInfo = () => apiClient.get('/db_info');

// You might want to add interceptors for automatic token handling if not using session cookies
// Or for centralized error handling
apiClient.interceptors.response.use(
    response => response,
    error => {
        console.error("API Error:", error.response?.data || error.message);
        // Potentially redirect to login if 401 Unauthorized
        if (error.response?.status === 401) {
            // Maybe trigger a global logout action here
            console.warn("Unauthorized request. Consider redirecting to login.");
            // Example: window.location.href = '/login'; (Hard redirect)
            // Or better: use context/state management to update auth status
        }
        return Promise.reject(error); // Important: reject the promise so components can catch it
    }
);

export default apiClient;