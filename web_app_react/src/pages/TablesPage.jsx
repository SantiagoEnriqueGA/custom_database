import React, { useState, useEffect, useCallback } from 'react';
import { listTables, dropTable, queryTable } from '../services/api';
import DataTable from '../components/DataTable'; // Create this component
import LoadingSpinner from '../components/LoadingSpinner';
import './TablesPage.css'; // Create basic styles

function TablesPage() {
    const [tables, setTables] = useState([]);
    const [selectedTable, setSelectedTable] = useState(null);
    const [tableData, setTableData] = useState(null);
    const [loadingTables, setLoadingTables] = useState(true);
    const [loadingData, setLoadingData] = useState(false);
    const [error, setError] = useState('');

    const fetchTables = useCallback(async () => {
        setLoadingTables(true);
        setError('');
        try {
            const response = await listTables();
            if (response.data?.status === 'success') {
                setTables(response.data.data || []);
            } else {
                throw new Error(response.data?.message || 'Failed to fetch tables');
            }
        } catch (err) {
            setError(err.message);
            setTables([]);
        } finally {
            setLoadingTables(false);
        }
    }, []);

    useEffect(() => {
        fetchTables();
    }, [fetchTables]);

    const handleViewTable = async (tableName) => {
        setSelectedTable(tableName);
        setLoadingData(true);
        setTableData(null);
        setError('');
        try {
            const response = await queryTable(tableName, null); // No filter for view all
             if (response.data?.status === 'success') {
                setTableData({
                    columns: response.data.columns || (response.data.data?.length > 0 ? Object.keys(response.data.data[0]) : []),
                    rows: response.data.data || []
                });
            } else {
                throw new Error(response.data?.message || `Failed to query table ${tableName}`);
            }
        } catch (err) {
            setError(`Error viewing table ${tableName}: ${err.message}`);
        } finally {
            setLoadingData(false);
        }
    };

    const handleDropTable = async (tableName) => {
         if (window.confirm(`Are you sure you want to drop table '${tableName}'?`)) {
             setError('');
             setLoadingTables(true); // Visually indicate action
             try {
                const response = await dropTable(tableName);
                if (response.data?.status === 'success') {
                    alert(`Table '${tableName}' dropped successfully.`);
                    setSelectedTable(null); // Clear selection if dropped
                    setTableData(null);
                    await fetchTables(); // Refresh table list
                } else {
                    throw new Error(response.data?.message || `Failed to drop table ${tableName}`);
                }
             } catch (err) {
                 setError(`Error dropping table ${tableName}: ${err.message}`);
                 setLoadingTables(false); // Re-enable list if drop failed
             }
             // setLoadingTables(false) is handled in fetchTables completion
         }
    };

    return (
        <div className="tables-page">
            <h1>Database Tables</h1>
            {error && <p className="error-message">{error}</p>}

            <div className="tables-layout">
                <div className="table-list-container">
                    <h2>Tables</h2>
                    {loadingTables ? <LoadingSpinner /> : (
                        <ul>
                            {tables.length > 0 ? tables.map(table => (
                                <li key={table} className={selectedTable === table ? 'selected' : ''}>
                                    <span>{table}</span>
                                    <div className="table-actions">
                                        <button onClick={() => handleViewTable(table)} disabled={loadingData}>View</button>
                                        <button onClick={() => handleDropTable(table)} className="danger" disabled={loadingTables}>Drop</button>
                                    </div>
                                </li>
                            )) : <li>No tables found.</li>}
                        </ul>
                    )}
                </div>

                <div className="table-data-container">
                    <h2>Table Data{selectedTable ? `: ${selectedTable}` : ''}</h2>
                    {loadingData ? <LoadingSpinner /> : (
                        tableData ? (
                            <DataTable columns={tableData.columns} data={tableData.rows} />
                        ) : (
                            <p>{selectedTable ? 'Select a table to view its data.' : 'No table selected.'}</p>
                        )
                    )}
                     {tableData && tableData.rows.length === 0 && <p>Table is empty.</p>}
                </div>
            </div>
        </div>
    );
}

export default TablesPage;