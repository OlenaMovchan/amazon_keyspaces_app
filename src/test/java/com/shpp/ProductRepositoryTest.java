package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ProductRepositoryTest {
    @Mock
    private CqlSession mockSession;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private Row mockRow;

    @Mock
    private Logger mockLogger;

    private ProductRepository productRepository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        productRepository = new ProductRepository(mockSession);
        when(mockSession.execute(any(SimpleStatement.class))).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(mockRow);
        when(mockLogger.isInfoEnabled()).thenReturn(true);
    }

    @Test
    void testCreateTable() {
        productRepository.createTable();
        verify(mockSession, times(1)).execute(any(SimpleStatement.class));
    }

    @Test
    void testInsertProductsInBatches() {
        int totalProducts = 10;
        int totalStores = 5;
        int totalCategories = 2;
        int batchSize = 3;

        ArgumentCaptor<BatchStatement> batchCaptor = ArgumentCaptor.forClass(BatchStatement.class);

        productRepository.insertProductsInBatches(totalProducts, totalStores, totalCategories, batchSize);

        verify(mockSession, atLeastOnce()).execute(batchCaptor.capture());
        //BatchStatement capturedBatch = batchCaptor.getValue();
        //List<BoundStatement> capturedStatements = capturedBatch.getStatements();
        //List<BoundStatement> capturedStatements = batchCaptor.getAllValues().get(0).getStatements();

        // Verify the correct number of statements are in the batch
       // assertEquals(totalProducts * totalStores * totalCategories, capturedStatements.size());

        // Verify the content of the first statement
      //  BoundStatement firstStatement = capturedStatements.get(0);
//        assertEquals("Category_0", firstStatement.getString("category_name"));
//        assertEquals("Store_0", firstStatement.getString("store_address"));
//        assertNotNull(firstStatement.getUuid("product_id"));
//        assertTrue(firstStatement.getInt("quantity") >= 0 && firstStatement.getInt("quantity") < 1000);
    }
}
//    ArgumentCaptor<BatchStatement> batchCaptor = ArgumentCaptor.forClass(BatchStatement.class);
//
//    productRepository.insertProductsInBatches(totalProducts, totalStores, totalCategories, batchSize);
//
//    verify(mockSession, atLeastOnce()).execute(batchCaptor.capture());
//    BatchStatement capturedBatch = batchCaptor.getValue();
//    List<BoundStatement> capturedStatements = capturedBatch.getStatements();
//
//    // Verify the correct number of statements are in the batch
//    assertEquals(totalProducts * totalStores * totalCategories, capturedStatements.size());
//
//    // Verify the content of the first statement
//    BoundStatement firstStatement = capturedStatements.get(0);
//    assertEquals("Category_0", firstStatement.getString("category_name"));
//    assertEquals("Store_0", firstStatement.getString("store_address"));
//    assertNotNull(firstStatement.getUuid("product_id"));
//    assertTrue(firstStatement.getInt("quantity") >= 0 && firstStatement.getInt("quantity") < 1000);