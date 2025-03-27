// src/pages/DatabaseObjects.jsx
import React, { useState, useEffect, useCallback } from 'react';
import {
    listTables, dropTable, queryTable,
    listViews, queryView,
    listMaterializedViews, queryMaterializedView
} from '../services/api';
import DataTable from '../components/DataTable';
import LoadingSpinner from '../components/LoadingSpinner';
import PaginationControls from '../components/PaginationControls'; // <-- Import PaginationControls
import './DatabaseObjects.css';

const SOURCE_TYPES = {
  TABLE: 'table',
  VIEW: 'view',
  MATERIALIZED_VIEW: 'materialized_view'
};

// --- Default Items Per Page ---
const DEFAULT_ITEMS_PER_PAGE = 15;

function DatabaseObjects() {
    const [tables, setTables] = useState([]);
    const [views, setViews] = useState([]);
    const [materializedViews, setMaterializedViews] = useState([]);

    const [selectedSourceName, setSelectedSourceName] = useState(null);
    const [selectedSourceType, setSelectedSourceType] = useState(null);

    // --- Store Full Source Data ---
    const [fullSourceData, setFullSourceData] = useState(null); // Store all rows here

    const [loadingLists, setLoadingLists] = useState(true);
    const [loadingData, setLoadingData] = useState(false);
    const [error, setError] = useState('');
    // --- Add Current Page State ---
    const [currentPage, setCurrentPage] = useState(1);
    const [itemsPerPage, setItemsPerPage] = useState(DEFAULT_ITEMS_PER_PAGE);

    const fetchLists = useCallback(async () => {
        setLoadingLists(true);
        setError('');
        setTables([]);
        setViews([]);
        setMaterializedViews([]);
        // Reset selection and data when lists are refreshed
        setSelectedSourceName(null);
        setSelectedSourceType(null);
        setFullSourceData(null);
        setCurrentPage(1);
        setItemsPerPage(DEFAULT_ITEMS_PER_PAGE); // Reset on full list refresh
        try {
            const results = await Promise.allSettled([
                listTables(),
                listViews(),
                listMaterializedViews()
            ]);

            const [tablesResult, viewsResult, mvsResult] = results;

            if (tablesResult.status === 'fulfilled' && tablesResult.value.data?.status === 'success') {
                setTables(tablesResult.value.data.data || []);
            } else {
                console.error("Failed to fetch tables:", tablesResult.reason || tablesResult.value?.data?.message);
            }

            if (viewsResult.status === 'fulfilled' && viewsResult.value.data?.status === 'success') {
                setViews(viewsResult.value.data.data || []);
            } else {
                console.error("Failed to fetch views:", viewsResult.reason || viewsResult.value?.data?.message);
            }

            if (mvsResult.status === 'fulfilled' && mvsResult.value.data?.status === 'success') {
                setMaterializedViews(mvsResult.value.data.data || []);
            } else {
                console.error("Failed to fetch materialized views:", mvsResult.reason || mvsResult.value?.data?.message);
            }

             if (results.some(r => r.status === 'rejected' || r.value?.data?.status !== 'success')) {
                 setError('Failed to fetch some database objects. Check console for details.');
             }

        } catch (err) {
            setError(`Error fetching lists: ${err.message}`);
            console.error("Error in fetchLists:", err);
        } finally {
            setLoadingLists(false);
        }
    }, []);

    useEffect(() => {
        fetchLists();
    }, [fetchLists]);

    // --- handleViewSource - Reset page ---
    const handleViewSource = async (sourceName, sourceType) => {
        setSelectedSourceName(sourceName);
        setSelectedSourceType(sourceType);
        setLoadingData(true);
        setFullSourceData(null); // Clear previous data
        setCurrentPage(1); // --- Reset page on view ---
        setError('');
        try {
            let response;
            switch (sourceType) {
                case SOURCE_TYPES.TABLE:
                    response = await queryTable(sourceName, null);
                    break;
                case SOURCE_TYPES.VIEW:
                    response = await queryView(sourceName);
                    break;
                case SOURCE_TYPES.MATERIALIZED_VIEW:
                    response = await queryMaterializedView(sourceName);
                    break;
                default:
                    throw new Error(`Unknown source type: ${sourceType}`);
            }

            if (response.data?.status === 'success') {
                // --- Set Full Source Data ---
                setFullSourceData({
                    columns: response.data.columns || (response.data.data?.length > 0 ? Object.keys(response.data.data[0]).filter(k => k !== '_record_id') : []),
                    rows: response.data.data || []
                });
            } else {
                throw new Error(response.data?.message || `Failed to query ${sourceType} ${sourceName}`);
            }
        } catch (err) {
            setError(`Error viewing ${sourceType} ${sourceName}: ${err.message}`);
            setFullSourceData(null); // Clear data on error
        } finally {
            setLoadingData(false);
        }
    };

    // --- handleDropSource - FetchLists resets state ---
    const handleDropSource = async (sourceName, sourceType) => {
        // Drop logic remains the same...
         if (sourceType !== SOURCE_TYPES.TABLE) {
            alert(`Dropping ${sourceType}s is not implemented in this interface yet.`);
            return;
        }
        if (window.confirm(`Are you sure you want to drop table '${sourceName}'?`)) {
            // ... (rest of drop logic)
            // Ensure fetchLists() is called on success to reset everything
            setError('');
            setLoadingLists(true); // Indicate activity
            try {
                const response = await dropTable(sourceName);
                if (response.data?.status === 'success') {
                    alert(`Table '${sourceName}' dropped successfully.`);
                    // Reset selection if the dropped item was selected
                    if (selectedSourceName === sourceName && selectedSourceType === sourceType) {
                        setSelectedSourceName(null);
                        setSelectedSourceType(null);
                        setFullSourceData(null);
                        setCurrentPage(1);
                    }
                    await fetchLists(); // Refresh all lists and reset state
                } else {
                    throw new Error(response.data?.message || `Failed to drop table ${sourceName}`);
                }
            } catch (err) {
                setError(`Error dropping table ${sourceName}: ${err.message}`);
                setLoadingLists(false); // Stop loading indicator on error if fetchLists isn't called
            }
        }
    };

    // --- Handler for Items Per Page Change ---
    const handleItemsPerPageChange = (newSize) => {
        setItemsPerPage(newSize);
        setCurrentPage(1); // Reset to first page
    };

    // ... renderListSection
    const renderListSection = (title, items, type) => (
         // Render logic for list sections remains the same...
         <div className="list-section">
             <h3>{title}</h3>
             {items.length > 0 ? (
                 <ul>
                     {items.map(item => (
                         <li
                             key={`${type}-${item}`}
                             className={selectedSourceName === item && selectedSourceType === type ? 'selected' : ''}
                         >
                             <span className="item-name">{item}</span>
                             <div className="table-actions">
                                 <button onClick={() => handleViewSource(item, type)} disabled={loadingData || loadingLists}>View</button>
                                 <button
                                     onClick={() => handleDropSource(item, type)}
                                     className="danger"
                                     disabled={loadingData || loadingLists || type !== SOURCE_TYPES.TABLE}
                                     title={type !== SOURCE_TYPES.TABLE ? "Drop only available for tables" : "Drop table"}
                                 >
                                     Drop
                                 </button>
                             </div>
                         </li>
                     ))}
                 </ul>
             ) : (
                 <p className="no-items-message">No {title.toLowerCase()} found.</p>
             )}
         </div>
    );

    // --- Update Pagination Calculation ---
    const totalItems = fullSourceData ? fullSourceData.rows.length : 0;
    // Ensure totalPages is at least 1 if there are items, 0 otherwise
    const totalPages = totalItems > 0 ? Math.ceil(totalItems / itemsPerPage) : 0;
    // Clamp currentPage
    const effectiveCurrentPage = Math.max(1, Math.min(currentPage, totalPages || 1));
    const startIndex = (effectiveCurrentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const currentPageData = fullSourceData ? fullSourceData.rows.slice(startIndex, endIndex) : [];

    // --- Update currentPage if it's out of bounds ---
    useEffect(() => {
        if(currentPage !== effectiveCurrentPage) {
            setCurrentPage(effectiveCurrentPage);
        }
    }, [currentPage, effectiveCurrentPage]);

    return (
        <div className="tables-page">
            <h1>Database Explorer</h1>
            {error && <p className="error-message">{error}</p>}

            <div className="tables-layout">
                {/* Left Panel: Lists */}
                <div className="table-list-container">
                    {/* ... list rendering ... */}
                     {loadingLists ? <LoadingSpinner /> : (
                        <>
                            {renderListSection("Tables", tables, SOURCE_TYPES.TABLE)}
                            {renderListSection("Views", views, SOURCE_TYPES.VIEW)}
                            {renderListSection("Materialized Views", materializedViews, SOURCE_TYPES.MATERIALIZED_VIEW)}
                        </>
                    )}
                    {!loadingLists && tables.length === 0 && views.length === 0 && materializedViews.length === 0 && (
                         <p>No tables, views, or materialized views found.</p>
                    )}
                </div>

                {/* Right Panel: Data Display */}
                <div className="table-data-container">
                    <h2>
                        {selectedSourceName
                            ? `Data for ${selectedSourceType}: ${selectedSourceName}`
                            : 'Select an object to view its data'}
                    </h2>
                    {loadingData ? <LoadingSpinner /> : (
                        fullSourceData ? (
                            <>
                                <DataTable columns={fullSourceData.columns} data={currentPageData} />

                                {/* --- Update PaginationControls Props --- */}
                                <PaginationControls
                                    currentPage={effectiveCurrentPage} // Use calculated page
                                    totalPages={totalPages}
                                    onPageChange={setCurrentPage}
                                    itemsPerPage={itemsPerPage}
                                    onItemsPerPageChange={handleItemsPerPageChange} // Pass handler
                                    totalItems={totalItems}
                                    disabled={loadingData || loadingLists} // Disable if loading lists or data
                                />
                                {totalItems === 0 && <p>Object is empty.</p>}
                            </>
                        ) : (
                            selectedSourceName && !error ? <p>Could not load data or object is empty.</p> : <p>Select an object to view its data.</p>
                        )
                    )}
                </div>
            </div>
        </div>
    );
}

export default DatabaseObjects;