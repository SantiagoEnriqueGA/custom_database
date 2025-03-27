import React from 'react';
import { Link } from 'react-router-dom';
import './NotFoundPage.css'; // Create basic CSS

function NotFoundPage() {
  return (
    <div className="not-found-page">
      <h1>404 - Not Found</h1>
      <p>Sorry, the page you are looking for does not exist.</p>
      <Link to="/">Go to Home Page</Link>
    </div>
  );
}

export default NotFoundPage;