package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.shpp.repository.DataSelection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

class DataSelectionTest {

    private static final String KEYSPACE_NAME = "my_keyspace";
    private static final String TABLE_NAME2 = "total_products_by_store_";

    @Mock
    private CqlSession sessionMock;

    @InjectMocks
    private DataSelection dataSelection;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testSelectData() {

        String category = "TestCategory";
        String selectDataQuery = String.format(
                "SELECT * FROM \"%s\".\"%s\" WHERE category_name = '%s';",
                KEYSPACE_NAME, TABLE_NAME2, category);

        ResultSet resultSetMock = mock(ResultSet.class);
        when(sessionMock.execute(selectDataQuery)).thenReturn(resultSetMock);

        List<Row> rows = Arrays.asList(
                mockRow(50L, "Store1", "TestCategory"),
                mockRow(30L, "Store2", "TestCategory")
        );

        when(resultSetMock.iterator()).thenReturn(rows.iterator());
        dataSelection.selectData(category);

        verify(sessionMock, times(1)).execute(selectDataQuery);
        verify(resultSetMock).iterator();
    }

    private Row mockRow(long totalQuantity, String storeAddress, String categoryName) {
        Row row = mock(Row.class);
        when(row.getLong("total_quantity")).thenReturn(totalQuantity);
        when(row.getString("store_address")).thenReturn(storeAddress);
        when(row.getString("category_name")).thenReturn(categoryName);
        return row;
    }
}
