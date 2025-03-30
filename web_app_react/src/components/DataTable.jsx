// src/components/DataTable.jsx
import React from 'react';
import './DataTable.css';

function DataTable({ columns = [], data = [] }) {
  // Initial checks remain the same
  if (!columns || !data) {
    return <p>No data or columns to display.</p>;
  }

  // Determine if the _record_id column should be displayed based on the data
  // Use safe check for hasOwnProperty to satisfy eslint:no-prototype-builtins
  const hasRecordIdColumn = data.some(row => row != null && Object.prototype.hasOwnProperty.call(row, '_record_id'));

  // Calculate the total number of columns for colSpan
  const totalColumns = columns.length + (hasRecordIdColumn ? 1 : 0);

  return (
    <div className="data-table-container">
      <table className="data-table">
        <thead>
          <tr>
            {/* Conditionally render the header for _record_id */}
            {hasRecordIdColumn && <th>_record_id</th>}
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {/* Handle empty data case within tbody for better structure */}
          {data.length === 0 ? (
            <tr>
              {/* Use colSpan to span across all potential columns */}
              <td colSpan={totalColumns} style={{ textAlign: 'center' }}>
                Table is empty.
              </td>
            </tr>
          ) : (
            data.map((row, rowIndex) => (
              <tr key={row?._record_id ?? rowIndex}>
                {/* Always render the <td> for _record_id if the column exists */}
                {hasRecordIdColumn && (
                  // Render the ID if present, otherwise an empty string/cell
                  <td>{row?._record_id ?? ''}</td>
                )}
                {columns.map((col) => (
                  <td key={col}>
                    {/* Safely render cell content, default to empty string */}
                    {typeof row[col] === 'object' && row[col] !== null
                      ? JSON.stringify(row[col])
                      : String(row[col] ?? '')}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
      {/* Optional: Remove the message below if the tbody message is sufficient */}
      {/* {data.length === 0 && <p>Table is empty.</p>} */}
    </div>
  );
}

export default DataTable;