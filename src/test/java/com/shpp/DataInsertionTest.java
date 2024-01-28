package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;

import com.shpp.dto.CategoryDto;
import com.shpp.dto.ProductDto;
import com.shpp.dto.StoreDto;
import com.shpp.repository.DataInsertion;
import com.shpp.repository.DataSelection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DataInsertionTest {

    @Mock
    private CqlSession session;
    @Mock
    private PreparedStatement preparedStatement;

    @InjectMocks
    DataInsertion dataInsertion;

    private final String keyspaceName = "test_keyspace";
    private final String productTable = "test_product_table";
    private final String storeTable = "test_store_table";
    private final String categoryTable = "test_category_table";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testInsertProductData() throws Exception {

        List<ProductDto> productData = new ArrayList<>();
        productData.add(new ProductDto(UUID.randomUUID(), "Product1", UUID.randomUUID()));
        productData.add(new ProductDto(UUID.randomUUID(), "Product2", UUID.randomUUID()));

        when(session.prepare(any(String.class))).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(UUID.class), any(String.class))).thenReturn(mock(BoundStatement.class));
        when(session.execute(any(BoundStatement.class))).thenReturn(mockResultSet());

        dataInsertion.insertProductData(session, productData, keyspaceName, productTable);

        verify(session).prepare(String.format(
                "INSERT INTO %s.%s (product_id, product_name) VALUES (?, ?)",
                keyspaceName, productTable));

        verify(preparedStatement, times(2)).bind(any(UUID.class), any(String.class));
    }

    @Test
    void testInsertStoreData() throws Exception {

        List<StoreDto> storeData = new ArrayList<>();
        storeData.add(new StoreDto(UUID.randomUUID(), "Store1"));
        storeData.add(new StoreDto(UUID.randomUUID(), "Store2"));

        when(session.prepare(any(String.class))).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(UUID.class), any(String.class))).thenReturn(mock(BoundStatement.class));
        when(session.execute(any(BoundStatement.class))).thenReturn(mockResultSet()); // Mock a successful execution

        dataInsertion.insertStoreData(session, storeData, keyspaceName, storeTable);

        verify(session).prepare(String.format(
                "INSERT INTO %s.%s (store_id, store_address) VALUES (?, ?)",
                keyspaceName, storeTable));

        verify(preparedStatement, times(2)).bind(any(UUID.class), any(String.class));
    }
    @Test
    void testInsertCategoryData() throws Exception {

        List<CategoryDto> categoryData = new ArrayList<>();
        categoryData.add(new CategoryDto(UUID.randomUUID(), "Category1"));
        categoryData.add(new CategoryDto(UUID.randomUUID(), "Category2"));

        when(session.prepare(any(String.class))).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(UUID.class), any(String.class))).thenReturn(mock(BoundStatement.class));
        when(session.execute(any(BoundStatement.class))).thenReturn(mockResultSet());

        dataInsertion.insertCategoryData(session, categoryData, keyspaceName, categoryTable);

        verify(session).prepare(String.format(
                "INSERT INTO %s.%s (category_id, category_name) VALUES (?, ?)",
                keyspaceName, categoryTable));

        verify(preparedStatement, times(2)).bind(any(UUID.class), any(String.class));
    }

    private ResultSet mockResultSet() {
        return mock(ResultSet.class);
    }

}
