import React, { useState, useEffect, useCallback } from 'react';
import {
    listTables, dropTable, queryTable,
    listViews, queryView, // Assuming dropView is not yet implemented/needed
    listMaterializedViews, queryMaterializedView // Assuming dropMV is not yet implemented/needed
} from '../services/api';
import DataTable from '../components/DataTable';
import LoadingSpinner from '../components/LoadingSpinner';
import './DatabaseObjects.css';

// Define source types for clarity
const SOURCE_TYPES = {
  TABLE: 'table',
  VIEW: 'view',
  MATERIALIZED_VIEW: 'materialized_view'
};

function TablesPage() {
    // State for lists
    const [tables, setTables] = useState([]);
    const [views, setViews] = useState([]);
    const [materializedViews, setMaterializedViews] = useState([]);

    // State for selection
    const [selectedSourceName, setSelectedSourceName] = useState(null);
    const [selectedSourceType, setSelectedSourceType] = useState(null);

    // State for data display
    const [sourceData, setSourceData] = useState(null); // Renamed from tableData

    // State for loading indicators
    const [loadingLists, setLoadingLists] = useState(true); // Combined loading for initial list fetch
    const [loadingData, setLoadingData] = useState(false);

    // State for errors
    const [error, setError] = useState('');

    const fetchLists = useCallback(async () => {
        setLoadingLists(true);
        setError('');
        setTables([]);
        setViews([]);
        setMaterializedViews([]);
        try {
            // Fetch all lists concurrently
            const results = await Promise.allSettled([
                listTables(),
                listViews(),
                listMaterializedViews()
            ]);

            // Process results
            const [tablesResult, viewsResult, mvsResult] = results;

            if (tablesResult.status === 'fulfilled' && tablesResult.value.data?.status === 'success') {
                setTables(tablesResult.value.data.data || []);
            } else {
                console.error("Failed to fetch tables:", tablesResult.reason || tablesResult.value?.data?.message);
                // Optionally set a specific error for tables
            }

            if (viewsResult.status === 'fulfilled' && viewsResult.value.data?.status === 'success') {
                setViews(viewsResult.value.data.data || []);
            } else {
                console.error("Failed to fetch views:", viewsResult.reason || viewsResult.value?.data?.message);
                // Optionally set a specific error for views
            }

            if (mvsResult.status === 'fulfilled' && mvsResult.value.data?.status === 'success') {
                setMaterializedViews(mvsResult.value.data.data || []);
            } else {
                console.error("Failed to fetch materialized views:", mvsResult.reason || mvsResult.value?.data?.message);
                // Optionally set a specific error for mvs
            }

             // Set a general error if any fetch failed (optional, could show partial data)
             if (results.some(r => r.status === 'rejected' || r.value?.data?.status !== 'success')) {
                 setError('Failed to fetch some database objects. Check console for details.');
             }

        } catch (err) {
            // Catch errors from Promise.all itself (unlikely with allSettled)
            setError(`Error fetching lists: ${err.message}`);
            console.error("Error in fetchLists:", err);
        } finally {
            setLoadingLists(false);
        }
    }, []);

    useEffect(() => {
        fetchLists();
    }, [fetchLists]);

    const handleViewSource = async (sourceName, sourceType) => {
        setSelectedSourceName(sourceName);
        setSelectedSourceType(sourceType);
        setLoadingData(true);
        setSourceData(null); // Clear previous data
        setError('');
        try {
            let response;
            switch (sourceType) {
                case SOURCE_TYPES.TABLE:
                    response = await queryTable(sourceName, null); // No filter for view all
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
                setSourceData({
                    columns: response.data.columns || (response.data.data?.length > 0 ? Object.keys(response.data.data[0]).filter(k => k !== '_record_id') : []),
                    rows: response.data.data || []
                });
            } else {
                throw new Error(response.data?.message || `Failed to query ${sourceType} ${sourceName}`);
            }
        } catch (err) {
            setError(`Error viewing ${sourceType} ${sourceName}: ${err.message}`);
            setSourceData(null); // Clear data on error
        } finally {
            setLoadingData(false);
        }
    };

    // Only handles dropping tables for now
    const handleDropSource = async (sourceName, sourceType) => {
        if (sourceType !== SOURCE_TYPES.TABLE) {
            alert(`Dropping ${sourceType}s is not implemented in this interface yet.`);
            return;
        }

        if (window.confirm(`Are you sure you want to drop table '${sourceName}'?`)) {
            setError('');
            // Show loading briefly (fetchLists will handle final state)
            // Maybe add a specific loading state for the item being dropped?
            setLoadingLists(true);
            try {
                const response = await dropTable(sourceName);
                if (response.data?.status === 'success') {
                    alert(`Table '${sourceName}' dropped successfully.`);
                    if (selectedSourceName === sourceName && selectedSourceType === sourceType) {
                        setSelectedSourceName(null);
                        setSelectedSourceType(null);
                        setSourceData(null);
                    }
                    await fetchLists(); // Refresh all lists
                } else {
                    throw new Error(response.data?.message || `Failed to drop table ${sourceName}`);
                }
            } catch (err) {
                setError(`Error dropping table ${sourceName}: ${err.message}`);
                setLoadingLists(false); // Ensure loading stops if fetchLists isn't called on error
            }
            // setLoadingLists(false) is handled by fetchLists completion
        }
    };

    // Helper to render a list section
    const renderListSection = (title, items, type) => (
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
                                <button onClick={() => handleViewSource(item, type)} disabled={loadingData}>View</button>
                                <button
                                    onClick={() => handleDropSource(item, type)}
                                    className="danger"
                                    disabled={loadingLists || type !== SOURCE_TYPES.TABLE} // Disable drop for non-tables
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


    return (
        <div className="tables-page">
            <h1>Database Explorer</h1> {/* Updated Title */}
            {error && <p className="error-message">{error}</p>}

            <div className="tables-layout">
                {/* Left Panel: Lists */}
                <div className="table-list-container">
                    <h2>Objects</h2>
                    {loadingLists ? <LoadingSpinner /> : (
                        <>
                            {renderListSection("Tables", tables, SOURCE_TYPES.TABLE)}
                            {renderListSection("Views", views, SOURCE_TYPES.VIEW)}
                            {renderListSection("Materialized Views", materializedViews, SOURCE_TYPES.MATERIALIZED_VIEW)}
                        </>
                    )}
                     {/* Show message if all lists are empty after loading */}
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
                        sourceData ? (
                            <DataTable columns={sourceData.columns} data={sourceData.rows} />
                        ) : (
                            selectedSourceName && !error && <p>Could not load data or object is empty.</p> // Show message if selected but no data/error
                        )
                    )}
                    {/* Message moved into the conditional rendering above */}
                    {/* {sourceData && sourceData.rows.length === 0 && <p>Object is empty.</p>} */}
                </div>
            </div>
        </div>
    );
}

export default TablesPage;