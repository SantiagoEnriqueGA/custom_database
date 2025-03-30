import React from 'react';
import { Link, useNavigate } from 'react-router-dom';
import useAuth from '../hooks/useAuth';
import './Navbar.css'; // Create basic styles

function Navbar() {
    const { isAuthenticated, logout } = useAuth();
    const navigate = useNavigate();

    const handleLogout = async () => {
        await logout();
        navigate('/login'); // Redirect to login after logout
    };

    return (
        <nav className="navbar">
            <Link to="/" className="navbar-brand">SegaDB Home</Link>
            <ul className="navbar-nav">
                {isAuthenticated ? (
                    <>
                        <li><Link to="/database-objects">Database Objects</Link></li> 
                        <li><Link to="/create-table">Create Table</Link></li>
                        <li><Link to="/insert-record">Insert Record</Link></li>
                        <li><Link to="/query-builder">Query Builder</Link></li>
                        <li><Link to="/create-procedure">Create Procedure</Link></li>
                        <li><Link to="/db-info">DB Info</Link></li>
                        <li><button onClick={handleLogout} className="logout-button">Logout</button></li>
                    </>
                ) : (
                    <li><Link to="/login">Login</Link></li>
                )}
            </ul>
        </nav>
    );
}

export default Navbar;