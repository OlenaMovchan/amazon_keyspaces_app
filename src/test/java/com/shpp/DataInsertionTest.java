package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.shpp.dto.Category;
import com.shpp.dto.Store;
import com.shpp.repository.DataInsertion;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.Mockito.*;

public class DataInsertionTest {

    @Test
    void testInsertData() {
        CqlSession session = mock(CqlSession.class);


        DataInsertion dataInsertion = new DataInsertion(session);


//            dataInsertion.insertData(session);
//
//
//            verify(session, atLeastOnce()).execute(any(BatchStatement.class));
    }

    @Test
    void testGenerateStoreData() {
        // Mocking the ValidatorClass
        ValidatorClass<Store> validatorClass = mock(ValidatorClass.class);
        when(validatorClass.validateDTO(any(Store.class))).thenReturn(Collections.emptySet());

        // Create a test instance of DataInsertion
//            DataInsertion dataInsertion = new DataInsertion(session, validatorClass);
//
//            // Call the method to be tested
//            String[] storeData = Repository.generateStoreData(5);
//
//            // Add assertions based on the expected behavior
//            assertEquals(5, storeData.length);
//            // Add more assertions as needed
    }

    @Test
    void testGenerateCategoryData() {

        ValidatorClass<Category> validatorClass = mock(ValidatorClass.class);
        when(validatorClass.validateDTO(any(Category.class))).thenReturn(Collections.emptySet());

//
//            DataInsertion dataInsertion = new DataInsertion(session, validatorClass);
//
//
//            String[] categoryData = dataInsertion.generateCategoryData(5);
//
//
//            assertEquals(5, categoryData.length);

    }
}


