package com.shpp;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.shpp.dto.CategoryDto;
import com.shpp.dto.ProductDto;
import com.shpp.dto.StoreDto;
import com.shpp.repository.DataIns;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;


class DataInsTest {

    private CqlSession mockSession;
    private DataIns dataIns;

    @BeforeEach
    void setUp() {
        mockSession = mock(CqlSession.class);

        dataIns = new DataIns(
                mockSession,
                "testKeyspace",
                "testStoreProductTable",
                "testTotalProductTable",
                "testProductTable",
                "testStoreTable",
                "testCategoryTable"
        );
    }

    @Test
    void insertStoreProductDataParallel_ShouldInsertDataInParallel() {
        List<StoreDto> storeData = Arrays.asList(
                new StoreDto(UUID.randomUUID(), "Store1"),
                new StoreDto(UUID.randomUUID(), "Store2")

        );
        List<ProductDto> productData = Arrays.asList(
                new ProductDto(UUID.randomUUID(), "Product1"),
                new ProductDto(UUID.randomUUID(), "Product2")
        );
        List<CategoryDto> categoryData = Arrays.asList(
                new CategoryDto(UUID.randomUUID(), "Category1"),
                new CategoryDto(UUID.randomUUID(), "Category2")
        );

        PreparedStatement mockPreparedStatement = mock(BoundStatement.class).getPreparedStatement();
        when(mockSession.prepare(anyString())).thenReturn((PreparedStatement) mockPreparedStatement);

        dataIns.insertStoreProductDataParallel(storeData, productData, categoryData);

        ArgumentCaptor<UUID> categoryCaptor = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<UUID> storeCaptor = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<UUID> productCaptor = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<Integer> quantityCaptor = ArgumentCaptor.forClass(Integer.class);

        verify(mockSession, atLeastOnce()).execute((Statement<?>) any());
        verify(mockPreparedStatement, atLeastOnce()).bind(
                categoryCaptor.capture(),
                storeCaptor.capture(),
                productCaptor.capture(),
                quantityCaptor.capture()
        );

        List<UUID> capturedCategories = categoryCaptor.getAllValues();
        List<UUID> capturedStores = storeCaptor.getAllValues();
        List<UUID> capturedProducts = productCaptor.getAllValues();
        List<Integer> capturedQuantities = quantityCaptor.getAllValues();

        assertEquals(storeData.size() * productData.size(), capturedCategories.size());
        assertEquals(storeData.size() * productData.size(), capturedStores.size());
        assertEquals(storeData.size() * productData.size(), capturedProducts.size());
        assertEquals(storeData.size() * productData.size(), capturedQuantities.size());
    }
}
