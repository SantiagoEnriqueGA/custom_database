// src/contexts/AuthContext.tsx

import React, { createContext, useState, useEffect, useCallback, ReactNode } from 'react';
import { checkAuth, login as apiLogin, logout as apiLogout } from '../services/api';
import LoadingSpinner from '../components/LoadingSpinner'; // Ensure this path is correct

// 1. Define the Type for the Context Value
interface AuthContextType {
  isAuthenticated: boolean;
  isLoading: boolean; // Tracks initial auth check
  login: (username: string, password: string) => Promise<boolean>; // Return true on success, throw on error
  logout: () => Promise<void>;
  verifyAuth: () => Promise<void>;
}

// 2. Create the Context with the Type and a Default Value
//    Providing a default value matching the type avoids null checks later in useAuth hook.
//    Alternatively, use `createContext<AuthContextType | null>(null)` and check for null in useAuth.
export const AuthContext = createContext<AuthContextType>({
    isAuthenticated: false,
    isLoading: true, // Start assuming loading
    login: async () => { throw new Error("AuthContext not initialized"); return false; },
    logout: async () => { throw new Error("AuthContext not initialized"); },
    verifyAuth: async () => { throw new Error("AuthContext not initialized"); },
});

// 3. Define the Type for the Provider's Props (including children)
interface AuthProviderProps {
  children: ReactNode; // Use ReactNode for children prop type
}

// 4. Type the AuthProvider component props
export const AuthProvider = ({ children }: AuthProviderProps) => {
    const [isAuthenticated, setIsAuthenticated] = useState<boolean>(false);
    const [isLoading, setIsLoading] = useState<boolean>(true); // Start loading until checked

    const verifyAuth = useCallback(async () => {
        // No need to setIsLoading(true) here, default state is true
        try {
            const response = await checkAuth();
            // Ensure response.data and response.data.logged_in exist (optional chaining)
            setIsAuthenticated(response?.data?.logged_in ?? false);
        } catch (error) {
            console.error("Auth check failed:", error);
            setIsAuthenticated(false); // Ensure unauthenticated on error
        } finally {
            // Set isLoading to false ONLY after the initial check completes
            setIsLoading(false);
        }
    }, []); // Empty dependency array is correct for running once on mount

    useEffect(() => {
        verifyAuth();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []); // Run only once on mount

    const login = async (username: string, password: string): Promise<boolean> => {
        // Loading state for login action should be handled in the component calling login
        try {
            await apiLogin(username, password);
            setIsAuthenticated(true); // Update global auth state
            return true; // Indicate success
        } catch (error) {
            console.error("Login failed:", error);
            setIsAuthenticated(false); // Ensure state is false on login failure
            throw error; // Re-throw for the component to handle UI feedback
        }
    };

    const logout = async (): Promise<void> => {
        // Loading state for logout action should be handled in the component calling logout
        try {
            await apiLogout();
        } catch (error) {
            console.error("Logout API call failed:", error);
            // Still proceed with client-side logout
        } finally {
            setIsAuthenticated(false); // Always update auth state on logout attempt
            // Optionally clear other user-related state/storage
        }
    };

    // Show loading spinner ONLY during the initial authentication check.
    if (isLoading) {
         // Make sure LoadingSpinner is a standard React component (value)
         return <LoadingSpinner />;
    }

    // Once initial loading is done, provide the context value and render children
    // Ensure the 'value' object matches the AuthContextType structure
    const contextValue: AuthContextType = {
        isAuthenticated,
        isLoading: isLoading, // Pass the initial loading state status if needed by consumers
        login,
        logout,
        verifyAuth
    };

    return (
        <AuthContext.Provider value={contextValue}>
            {children}
        </AuthContext.Provider>
    );
};