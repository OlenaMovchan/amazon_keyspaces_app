package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.shpp.repository.CreationTables;
import com.shpp.repository.DataSelection;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class AmazonKeyspacesAppTest {
//
//    private static final String TABLE_NAME = "store_product_table_";
//    private static final String TABLE_NAME2 = "total_products_by_store_";
//
//    private CqlSession session;
//    private Logger logger;
//
//    @BeforeEach
//    void setUp() {
//        // Mocking CqlSession and Logger
//        session = mock(CqlSession.class);
//        logger = mock(Logger.class);
//    }
//
//    @Test
//    void testRun() {
//        AmazonKeyspacesApp app = spy(AmazonKeyspacesApp.class);
//        doNothing().when(app).waitForTableCreation(any(), any());
//
//        app.LOGGER = logger;
//
//        // Mock the connector
//        Connector connector = mock(Connector.class);
//        when(connector.getSession()).thenReturn(session);
//
//        // Mock the creation tables and data selection
//        CreationTables creationTables = mock(CreationTables.class);
//        //whenNew(CreationTables.class).withNoArguments().thenReturn(creationTables);
//
//        DataSelection dataSelection = mock(DataSelection.class);
//        //whenNew(DataSelection.class).withArguments(session).thenReturn(dataSelection);
//
//        StopWatch stopWatch = mock(StopWatch.class);
//        //whenNew(StopWatch.class).withNoArguments().thenReturn(stopWatch);
//        Repository repository = mock(Repository.class);
//        app.run();
//
//        verify(connector).connect(anyString(), anyInt());
//        verify(creationTables).createTable(session);
//        verify(app).waitForTableCreation(session, TABLE_NAME);
//        verify(app).waitForTableCreation(session, TABLE_NAME2);
//        verify(stopWatch, times(3)).start();
//        verify(stopWatch, times(3)).stop();
//        verify(logger, times(4)).info(anyString());
//        verify(session, times(2)).execute(anyString());
//        verify(repository).insertData(session);
//        verify(dataSelection).selectData(anyString());
//        verify(connector).close();
//    }
}
