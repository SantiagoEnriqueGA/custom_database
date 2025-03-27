// src/pages/QueryBuilderPage.jsx
import React, { useState, useEffect, useCallback } from 'react';
import {
  listTables, queryTable,
  listViews, queryView,
  listMaterializedViews, queryMaterializedView
} from '../services/api';
import DataTable from '../components/DataTable';
import LoadingSpinner from '../components/LoadingSpinner';
import PaginationControls from '../components/PaginationControls'; // <-- Import PaginationControls
import './QueryBuilderPage.css';

const SOURCE_TYPES = {
  TABLE: 'table',
  VIEW: 'view',
  MATERIALIZED_VIEW: 'materialized_view'
};

// --- Default Items Per Page ---
const DEFAULT_ITEMS_PER_PAGE = 15;

function QueryBuilderPage() {
  const [sourceType, setSourceType] = useState(SOURCE_TYPES.TABLE);
  const [sourcesList, setSourcesList] = useState([]);
  const [selectedSource, setSelectedSource] = useState('');
  const [filterCondition, setFilterCondition] = useState('');
  // --- Store Full Query Result ---
  const [fullQueryResult, setFullQueryResult] = useState(null); // Store all rows here
  const [loadingSources, setLoadingSources] = useState(true);
  const [loadingQuery, setLoadingQuery] = useState(false);
  const [error, setError] = useState('');
  // --- Add Current Page State ---
  const [currentPage, setCurrentPage] = useState(1);
  // --- Add Items Per Page State ---
  const [itemsPerPage, setItemsPerPage] = useState(DEFAULT_ITEMS_PER_PAGE);

  const fetchSources = useCallback(async () => {
    setLoadingSources(true);
    setError('');
    setSourcesList([]);
    setSelectedSource('');
    setFullQueryResult(null); // Clear previous full results
    setCurrentPage(1); // --- Reset page on source change ---
    setItemsPerPage(DEFAULT_ITEMS_PER_PAGE); // Reset on source type change
    try {
      let response;
      if (sourceType === SOURCE_TYPES.TABLE) {
        response = await listTables();
      } else if (sourceType === SOURCE_TYPES.VIEW) {
        response = await listViews();
      } else if (sourceType === SOURCE_TYPES.MATERIALIZED_VIEW) {
        response = await listMaterializedViews();
      } else {
        throw new Error("Invalid source type selected");
      }

      if (response.data?.status === 'success') {
          const dataList = response.data.data || [];
          if (sourceType === SOURCE_TYPES.TABLE) {
              setSourcesList(dataList.filter(t => !t.startsWith('_')));
          } else {
              setSourcesList(dataList);
          }
      } else {
        throw new Error(response.data?.message || `Failed to fetch ${sourceType}s`);
      }
    } catch (err) {
      setError(`Error fetching sources: ${err.message}`);
      setSourcesList([]);
    } finally {
      setLoadingSources(false);
    }
  }, [sourceType]);

  useEffect(() => {
    fetchSources();
  }, [fetchSources]);

  // --- handleQuery - Reset page ---
  const handleQuery = async (e) => {
    e.preventDefault();
    if (!selectedSource) {
      setError('Please select a source to query.');
      return;
    }

    setLoadingQuery(true);
    setError('');
    setFullQueryResult(null); // Clear previous full results
    setCurrentPage(1); // --- Reset page on new query ---
    try {
      let response;
      const isTable = sourceType === SOURCE_TYPES.TABLE;
      const filterToSend = isTable && filterCondition.trim() !== '' ? filterCondition.trim() : null;

      if (sourceType === SOURCE_TYPES.TABLE) {
        response = await queryTable(selectedSource, filterToSend);
      } else if (sourceType === SOURCE_TYPES.VIEW) {
        response = await queryView(selectedSource);
      } else if (sourceType === SOURCE_TYPES.MATERIALIZED_VIEW) {
        response = await queryMaterializedView(selectedSource);
      } else {
          throw new Error("Invalid source type for query");
      }

      if (response.data?.status === 'success') {
        // --- Set Full Query Result ---
        setFullQueryResult({
          columns: response.data.columns || (response.data.data?.length > 0 ? Object.keys(response.data.data[0]).filter(k => k !== '_record_id') : []),
          rows: response.data.data || []
        });
      } else {
        throw new Error(response.data?.message || `Failed to query ${sourceType} ${selectedSource}`);
      }
    } catch (err) {
      setError(`Query Error: ${err.message}`);
    } finally {
      setLoadingQuery(false);
    }
  };

  //  --- Handler for Items Per Page Change ---
  const handleSourceTypeChange = (e) => {
      setSourceType(e.target.value);
      // Fetching is handled by the useEffect hook reacting to sourceType change
  };

  // --- Handler for Items Per Page Change ---
  const handleItemsPerPageChange = (newSize) => {
    setItemsPerPage(newSize);
    setCurrentPage(1); // Reset to first page when page size changes
  };

  const isFilterEnabled = sourceType === SOURCE_TYPES.TABLE;

  // --- Calculate Pagination Data ---
  const totalItems = fullQueryResult ? fullQueryResult.rows.length : 0;
  // Ensure totalPages is at least 1 if there are items, 0 otherwise
  const totalPages = totalItems > 0 ? Math.ceil(totalItems / itemsPerPage) : 0;
  // Clamp currentPage if it becomes invalid after changing itemsPerPage or data
  const effectiveCurrentPage = Math.max(1, Math.min(currentPage, totalPages || 1));
  const startIndex = (effectiveCurrentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const currentPageData = fullQueryResult ? fullQueryResult.rows.slice(startIndex, endIndex) : [];

  // --- Update currentPage if it's out of bounds after calculation ---
  useEffect(() => {
    if(currentPage !== effectiveCurrentPage) {
        setCurrentPage(effectiveCurrentPage);
    }
  }, [currentPage, effectiveCurrentPage]);

  return (
    <div className="query-builder-page">
      <h1>Query Builder</h1>

      <form onSubmit={handleQuery} className="query-form">
         {/* Form elements remain the same */}
         {error && <p className="error-message">{error}</p>}
          <div className="form-group">
              <label htmlFor="sourceType">Source Type:</label>
              <select
                  id="sourceType"
                  value={sourceType}
                  onChange={handleSourceTypeChange}
                  disabled={loadingSources || loadingQuery}
              >
                  <option value={SOURCE_TYPES.TABLE}>Table</option>
                  <option value={SOURCE_TYPES.VIEW}>View</option>
                  <option value={SOURCE_TYPES.MATERIALIZED_VIEW}>Materialized View</option>
              </select>
          </div>
          <div className="form-group">
              <label htmlFor="sourceName">Source Name:</label>
              {loadingSources ? <LoadingSpinner /> : (
                <select
                  id="sourceName"
                  value={selectedSource}
                  onChange={(e) => setSelectedSource(e.target.value)}
                  required
                  disabled={loadingQuery || loadingSources || sourcesList.length === 0}
                >
                  <option value="" disabled>-- Select a {sourceType.replace('_', ' ')} --</option>
                  {sourcesList.map(source => (
                    <option key={source} value={source}>{source}</option>
                  ))}
                  {sourcesList.length === 0 && <option disabled>No {sourceType.replace('_', ' ')}s available</option>}
                </select>
              )}
          </div>
          <div className="form-group">
              <label htmlFor="filterCondition">Filter Condition (Tables Only):</label>
              <input
                type="text"
                id="filterCondition"
                value={filterCondition}
                onChange={(e) => setFilterCondition(e.target.value)}
                disabled={loadingQuery || loadingSources || !isFilterEnabled}
                placeholder={isFilterEnabled ? "e.g., lambda record: record['price'] > 100" : "Filtering not applicable"}
              />
              {isFilterEnabled && <small style={{color: '#aaa'}}>Warning: Backend executes filter strings. Use with caution.</small>}
          </div>
          <button type="submit" disabled={loadingQuery || loadingSources || !selectedSource}>
              {loadingQuery ? <LoadingSpinner /> : 'Run Query'}
          </button>
      </form>

      <div className="query-results">
        <h2>Results {selectedSource ? `for ${sourceType.replace('_', ' ')}: ${selectedSource}` : ''}</h2>
        {loadingQuery ? <LoadingSpinner /> : (
          fullQueryResult ? ( // Check fullQueryResult instead of currentPageData
            <>
                {/* Pass Sliced Data to DataTable */}
                <DataTable columns={fullQueryResult.columns} data={currentPageData} />

                {/* --- Update PaginationControls Props --- */}
                <PaginationControls
                    currentPage={effectiveCurrentPage} // Use calculated effective page
                    totalPages={totalPages}
                    onPageChange={setCurrentPage}
                    itemsPerPage={itemsPerPage}
                    onItemsPerPageChange={handleItemsPerPageChange} // Pass handler
                    totalItems={totalItems}
                    disabled={loadingQuery} // Disable controls while loading new query
                />
                {totalItems === 0 && <p>Query returned no matching records.</p>}
            </>
          ) : (
            <p>Run a query to see results.</p>
          )
        )}
      </div>
    </div>
  );
}

export default QueryBuilderPage;