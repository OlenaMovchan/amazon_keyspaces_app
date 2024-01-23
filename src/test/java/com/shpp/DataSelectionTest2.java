package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.shpp.repository.DataSelection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class DataSelectionTest2 {
    private static final String KEYSPACE_NAME = "my_keyspace";
    private static final String TABLE_NAME2 = "total_products_by_store_";

    @Mock
    private CqlSession session;
    @InjectMocks
    DataSelection dataSelection;
    @Mock
    private Logger logger;

    @Test
    void testSelectData() {

        String category = "TestCategory";
        String selectDataQuery = String.format(
                "SELECT * FROM \"%s\".\"%s\" WHERE category_name = '" + category + "';",
                "my_keyspace", "total_products_by_store_");

        ResultSet resultSetMock = mock(ResultSet.class);
        when(session.execute(selectDataQuery)).thenReturn(resultSetMock);

        dataSelection.selectData("TestCategory");

        verify(session, times(1)).execute(anyString());

    }

}
