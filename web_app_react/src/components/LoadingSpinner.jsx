import React from 'react';
import './LoadingSpinner.css'; // We'll create this CSS file next

/**
 * A simple CSS-based loading spinner component.
 */
const LoadingSpinner = () => {
  // This div will be styled by the CSS to create the visual spinner
  return <div className="loading-spinner" aria-label="Loading..."></div>;
  // Note: aria-label provides context for screen readers.
  // Consider adding visually hidden text or more robust ARIA attributes
  // if the spinner appears without other context (like button text changing to "Loading...").
};

export default LoadingSpinner;