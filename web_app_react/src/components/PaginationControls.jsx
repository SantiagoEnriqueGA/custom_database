// src/components/PaginationControls.jsx
import React from 'react';
import './PaginationControls.css';

// Define common options for rows per page
const ROWS_PER_PAGE_OPTIONS = [10, 15, 25, 50, 100];

/**
 * Renders pagination controls including page navigation and rows per page selection.
 *
 * Props:
 * - currentPage (number): The currently active page (1-based).
 * - totalPages (number): The total number of pages available.
 * - onPageChange (function): Callback function invoked with the new page number.
 * - itemsPerPage (number): Current number of items shown per page.
 * - onItemsPerPageChange (function): Callback function invoked with the new itemsPerPage value.
 * - totalItems (number): Total number of items in the dataset.
 * - disabled (boolean, optional): Disables all controls if true.
 */
function PaginationControls({
    currentPage,
    totalPages,
    onPageChange,
    itemsPerPage,
    onItemsPerPageChange,
    totalItems = 0,
    disabled = false
}) {
    // Don't render controls if there are no items or just one page with default itemsPerPage
    // Or always render if totalPages > 1? Let's keep it simple for now: render if > 1 page
     if (totalPages <= 1 && totalItems <= ROWS_PER_PAGE_OPTIONS[0]) {
         // If only one page AND total items is less than or equal to the smallest per-page option,
         // no controls are really needed. Otherwise, show rows per page selector even for one page.
         // Maybe always show if totalItems > 0? Let's refine: show if totalPages > 1 OR if rows selector is needed
        // Simplified: Only hide if totalPages <= 1. The selector might still be useful.
        // if (totalPages <= 1) return null;
     }
     // More robust: Render if there's more than one page OR if there are items to select page size for
     if (totalPages <= 1 && totalItems <= Math.min(...ROWS_PER_PAGE_OPTIONS)) {
          return null; // Hide only if very few items and one page
     }


    const handlePrevious = () => {
        if (currentPage > 1) {
            onPageChange(currentPage - 1);
        }
    };

    const handleNext = () => {
        if (currentPage < totalPages) {
            onPageChange(currentPage + 1);
        }
    };

    const handleFirst = () => {
        if (currentPage > 1) {
            onPageChange(1);
        }
    };

    const handleLast = () => {
        if (currentPage < totalPages) {
            onPageChange(totalPages);
        }
    };

    const handleItemsPerPageSelect = (e) => {
        const newSize = parseInt(e.target.value, 10);
        onItemsPerPageChange(newSize);
    };

    // Calculate item range display
    const startItem = totalItems > 0 ? (currentPage - 1) * itemsPerPage + 1 : 0;
    const endItem = Math.min(currentPage * itemsPerPage, totalItems);

    return (
        <div className={`pagination-controls ${disabled ? 'disabled' : ''}`}>
             {/* Rows per page selector */}
            <div className="pagination-rows-selector">
                <label htmlFor="rowsPerPage">Rows:</label>
                <select
                    id="rowsPerPage"
                    value={itemsPerPage}
                    onChange={handleItemsPerPageSelect}
                    disabled={disabled}
                >
                    {ROWS_PER_PAGE_OPTIONS.map(size => (
                        <option key={size} value={size}>{size}</option>
                    ))}
                </select>
            </div>

            {/* Item range info */}
             <span className="pagination-info">
                 {totalItems > 0 ? `Showing ${startItem}-${endItem} of ${totalItems}` : 'No items'}
             </span>

            {/* Page navigation buttons (only if multiple pages) */}
            {totalPages > 1 && (
                 <div className="pagination-buttons">
                     <button
                         onClick={handleFirst}
                         disabled={disabled || currentPage === 1}
                         className="pagination-button edge"
                         aria-label="Go to first page"
                         title="First Page"
                     >
                         ««
                     </button>
                     <button
                         onClick={handlePrevious}
                         disabled={disabled || currentPage === 1}
                         className="pagination-button prev"
                         aria-label="Go to previous page"
                     >
                         « Prev
                     </button>
                     <span className="pagination-page-indicator">
                         Page {currentPage} of {totalPages}
                     </span>
                     <button
                         onClick={handleNext}
                         disabled={disabled || currentPage === totalPages}
                         className="pagination-button next"
                         aria-label="Go to next page"
                     >
                         Next »
                     </button>
                     <button
                         onClick={handleLast}
                         disabled={disabled || currentPage === totalPages}
                         className="pagination-button edge"
                         aria-label="Go to last page"
                         title="Last Page"
                     >
                         »»
                     </button>
                 </div>
            )}
        </div>
    );
}

export default PaginationControls;