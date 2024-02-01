package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.shpp.repository.DataSelection;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class DataSelectionTest {

    @Mock
    private CqlSession session;
    @InjectMocks
    private DataSelection dataSelection;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testFindCategoryIdByName() {

        String categoryName = "TestCategory";
        UUID categoryId = UUID.fromString("cbc45708-ee76-4542-baad-e600a156e109");

        ResultSet resultSet = mockResultSet();
        when(session.execute(any(String.class), any(String.class))).thenReturn(resultSet);

        UUID result = dataSelection.findCategoryIdByName(categoryName);

        assertEquals(categoryId, result);
    }

    @Test
    void testFindLargestQuantityAndStoreId() {

        UUID categoryId = UUID.fromString("cbc45708-ee76-4542-baad-e600a156e109");
        UUID storeId = UUID.fromString("cbc45708-ee76-4542-baad-e600a156e109");
        long largestQuantity = 100;

        ResultSet resultSet = mockResultSet(storeId, largestQuantity);
        when(session.execute(any(String.class), any(UUID.class))).thenReturn(resultSet);

        Pair<UUID, Long> result = dataSelection.findLargestQuantityAndStoreId(categoryId);

        assertEquals(Pair.of(storeId, largestQuantity), result);
    }

    private ResultSet mockResultSet(UUID storeId, long largestQuantity) {
        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(row.getLong("total_quantity")).thenReturn(largestQuantity);
        when(row.getUuid("store_id")).thenReturn(storeId);
        when(resultSet.iterator()).thenReturn(Collections.singletonList(row).iterator());
        return resultSet;
    }

    @Test
    void testFindAddressByStoreId() {

        UUID storeId = UUID.randomUUID();
        String storeAddress = "Test Address";

        ResultSet resultSet = mockResultSet();
        when(session.execute(any(String.class), any(UUID.class))).thenReturn(resultSet);

        String result = dataSelection.findAddressByStoreId(storeId);

        assertEquals(storeAddress, result);
    }

    private ResultSet mockResultSet() {
        ResultSet resultSet = mock(ResultSet.class);
        Row row = mock(Row.class);
        when(row.getUuid(any(String.class))).thenReturn(UUID.fromString("cbc45708-ee76-4542-baad-e600a156e109"));
        when(row.getString(any(String.class))).thenReturn("Test Address");
        when(row.getLong(any(String.class))).thenReturn(100L);
        when(resultSet.one()).thenReturn(row);
        return resultSet;
    }

}

