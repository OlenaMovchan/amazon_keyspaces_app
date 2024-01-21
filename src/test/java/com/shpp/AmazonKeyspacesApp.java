package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.datastax.oss.driver.api.core.cql.*;
import org.junit.jupiter.api.*;


import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AmazonKeyspacesApp {
    private static final String KEYSPACE_NAME = "test_keyspace";
    private static final String TABLE_NAME = "test_table";
    private static final Logger LOGGER = Mockito.mock(Logger.class);

    private static CqlSession mockSession;

    @BeforeAll
    static void setUp() {
        mockSession = Mockito.mock(CqlSession.class);
    }

//    @Test
//    void testWaitForTableCreation_Success() {
//        // Set up mock behavior for isTableCreated to return true immediately
//        when(mockSession.execute(anyString())).thenReturn(Mockito.mock(ResultSet.class));
//
//        // Invoke the method
//        assertDoesNotThrow(() -> AmazonKeyspacesApp.waitForTableCreation(mockSession, TABLE_NAME));
//
//        // Verify that the method logs the success message
//        verify(LOGGER).info("Table '{}' has been created", TABLE_NAME);
//    }
//
//    @Test
//    void testWaitForTableCreation_Timeout() {
//        // Set up mock behavior for isTableCreated to return false
//        when(mockSession.execute(anyString())).thenReturn(Mockito.mock(ResultSet.class, RETURNS_DEEP_STUBS));
//
//        // Invoke the method
//        RuntimeException exception = assertThrows(RuntimeException.class,
//                () -> AmazonKeyspacesApp.waitForTableCreation(mockSession, TABLE_NAME));
//
//        // Verify that the exception message indicates a timeout
//        assertTrue(exception.getMessage().contains("Timeout waiting for table creation"));
//    }
//
//    @Test
//    void testIsTableCreated_True() {
//        // Set up mock behavior for the ResultSet
//        ResultSet resultSet = Mockito.mock(ResultSet.class);
//        when(resultSet.all()).thenReturn(Mockito.mock(Row.class));
//
//        // Set up mock behavior for the execute method
//        when(mockSession.execute(anyString())).thenReturn(resultSet);
//
//        // Invoke the method
//        assertTrue(AmazonKeyspacesApp.isTableCreated(mockSession, TABLE_NAME));
//    }
//
//    @Test
//    void testIsTableCreated_False() {
//        // Set up mock behavior for an empty ResultSet
//        ResultSet resultSet = Mockito.mock(ResultSet.class);
//        when(resultSet.all()).thenReturn(null);
//
//        // Set up mock behavior for the execute method
//        when(mockSession.execute(anyString())).thenReturn(resultSet);
//
//        // Invoke the method
//        assertFalse(AmazonKeyspacesApp.isTableCreated(mockSession, TABLE_NAME));
//    }
//
//    @AfterAll
//    static void tearDown() {
//        // Close the mock session
//        if (mockSession != null) {
//            mockSession.close();
//        }
//    }
}
