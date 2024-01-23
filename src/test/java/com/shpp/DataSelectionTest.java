package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.shpp.repository.DataSelection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;


public class DataSelectionTest {

    @Mock
    private CqlSession session;
    @InjectMocks
    DataSelection dataSelection;
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    @Test
    public void testSelectData() {
        // Arrange
        String category = "TestCategory";
        String selectDataQuery = String.format(
                "SELECT * FROM \"%s\".\"%s\" WHERE category_name = '" + category + "';",
                "my_keyspace", "total_products_by_store_");

        ResultSet resultSetMock = mock(ResultSet.class);
        when(session.execute(selectDataQuery)).thenReturn(resultSetMock);

        List<Row> rows = Arrays.asList(
                mockRow(50L, "Store1", "TestCategory"),
                mockRow(30L, "Store2", "TestCategory")
        );

        when(resultSetMock.iterator()).thenReturn(rows.iterator());

        // Act
       // DataSelection.selectData(category);

        // Assert - Verify that the correct information is printed to the logger
//        verify(DataSelection.LOGGER).info("Category_name: {}, Store_address: {}, Total_Quantity: {}",
//                category, rows.get(0).getString("store_address"), 50L);

        // Verify that session.execute is called with the correct query
        verify(session).execute(selectDataQuery);

        // Verify that resultSet.iterator is called
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
