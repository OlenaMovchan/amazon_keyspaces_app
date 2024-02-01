package com.shpp;

import com.shpp.dto.CategoryDto;
import com.shpp.dto.ProductDto;
import com.shpp.dto.StoreDto;
import com.shpp.repository.DataGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class DataGeneratorTest {
    DataGenerator dataGenerator = new DataGenerator();
    public static Stream<Integer> provideTotalDto() {
        return Stream.of(5, 10, 15);
    }

    @ParameterizedTest
    @MethodSource("provideTotalDto")
    void testGenerateProductData(int totalProducts) {

        List<CategoryDto> categories = dataGenerator.generateCategoryData(3);
        List<ProductDto> products = dataGenerator.generateProductData(totalProducts, categories);

        assertNotNull(products);
        assertEquals(totalProducts, products.size());

        for (ProductDto product : products) {
            assertNotNull(product.getProductId());
            assertNotNull(product.getCategoryId());

            if (product.getName() != null) {
                assertTrue(product.getName().length() > 0);
            }
        }
    }
    @ParameterizedTest
    @MethodSource("provideTotalDto")
    void testGenerateStoreData(int totalStores) {

        List<StoreDto> stores = dataGenerator.generateStoreData(totalStores);

        assertNotNull(stores);
        assertEquals(totalStores, stores.size());

        for (StoreDto store : stores) {
            assertNotNull(store.getStoreId());
            assertNotNull(store.getLocation());
        }
    }

    @ParameterizedTest
    @MethodSource("provideTotalDto")
    void testGenerateCategoryData(int totalCategories) {

        List<CategoryDto> categories = dataGenerator.generateCategoryData(totalCategories);

        assertNotNull(categories);
        assertEquals(totalCategories, categories.size());

        for (CategoryDto category : categories) {
            assertNotNull(category.getCategoryId());
            assertNotNull(category.getCategoryName());
        }
    }

}
