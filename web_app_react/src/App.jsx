import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Navbar from './components/Navbar';
import ProtectedRoute from './components/ProtectedRoute';

// Import Pages
import HomePage from './pages/HomePage';
import LoginPage from './pages/LoginPage';
import DatabaseObjects from './pages/DatabaseObjects';
import CreateTablePage from './pages/CreateTablePage';
import QueryBuilderPage from './pages/QueryBuilderPage';
import InsertRecordPage from './pages/InsertRecordPage';
import CreateProcedurePage from './pages/CreateProcedurePage';
import DbInfoPage from './pages/DbInfoPage';
import NotFoundPage from './pages/NotFoundPage';

function App() {
  return (
    <div className="App">
      <Navbar />
      <main className="container"> {/* Add some basic layout */}
        <Routes>
          {/* Public Route */}
          <Route path="/login" element={<LoginPage />} />

          {/* Protected Routes */}
          <Route element={<ProtectedRoute />}>
            <Route path="/" element={<HomePage />} />
            <Route path="/database-objects" element={<DatabaseObjects />} />
            <Route path="/create-table" element={<CreateTablePage />} />
            <Route path="/query-builder" element={<QueryBuilderPage />} />
            <Route path="/insert-record" element={<InsertRecordPage />} />
            <Route path="/create-procedure" element={<CreateProcedurePage />} />
            <Route path="/db-info" element={<DbInfoPage />} />
          </Route>

          {/* Catch All Not Found Route */}
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </main>
    </div>
  );
}

export default App;