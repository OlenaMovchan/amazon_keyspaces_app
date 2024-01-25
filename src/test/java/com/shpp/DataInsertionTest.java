package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.shpp.repository.DataInsertion;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataInsertionTest {

    @Mock
    private CqlSession sessionMock;

    @Mock
    private ExecutorService executorServiceMock;

    @InjectMocks
    private DataInsertion dataInsertion;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGenerateStoreData() {

        String[] storeAddresses = dataInsertion.generateStoreData(3);

        assertEquals(3, storeAddresses.length);
    }

    @Test
    void testGenerateCategoryData() {

        String[] categories = dataInsertion.generateCategoryData(2);

        assertEquals(2, categories.length);
    }

}


