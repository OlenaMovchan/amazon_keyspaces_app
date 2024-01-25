package com.shpp.repository;

import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.shpp.dto.CategoryDto;
import com.shpp.dto.ProductDto;
import com.shpp.dto.StoreDto;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;

public class DataIns {

    private final CqlSession session;
    private final String keyspaceName;
    private final String storeProductTable;
    private final String totalProductTable;
    private String categoryTable;
    private String storeTable;
    private String productTable;
    int totalProducts = 1000;
    int totalCategories = 100;
    int totalStores = 25;
    DataGenerator dataGenerator = new DataGenerator();
    List<CategoryDto> categoryData = dataGenerator.generateCategoryData(totalCategories);
    List<StoreDto> storeData = dataGenerator.generateStoreData(totalStores);
    List<ProductDto> productData = dataGenerator.generateProductData(totalProducts);

    public DataIns(CqlSession session, String keyspaceName, String storeProductTable, String totalProductTable, String productTable, String storeTable, String categoryTable) {
        this.session = session;
        this.keyspaceName = keyspaceName;
        this.storeProductTable = storeProductTable;
        this.totalProductTable = totalProductTable;
        this.productTable = productTable;
        this.storeTable = storeTable;
        this.categoryTable = categoryTable;
    }
    public void insertStoreProductDataParallel(List<StoreDto> storeData, List<ProductDto> productData, List<CategoryDto> categoryData) {
        String insertQuery = String.format(
                "INSERT INTO %s.%s (category_id, store_id, product_id, quantity) VALUES (?, ?, ?, ?)",
                keyspaceName, storeProductTable);
        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_id = ? AND store_id = ?",
                keyspaceName, totalProductTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        PreparedStatement preparedStatementUpdate = session.prepare(updateTotalQuery);

        ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors()); // Create a ForkJoinPool
        forkJoinPool.submit(() ->
                storeData.parallelStream().forEach(storeDto ->
                        productData.parallelStream().forEach(productDto -> {
                            CategoryDto randomCategory = getRandomElement(categoryData);
                            int quantity = new Random().nextInt(100);

                            session.execute(preparedStatement.bind()
                                    .setUuid("category_id", randomCategory.getCategoryId())
                                    .setUuid("store_id", storeDto.getStoreId())
                                    .setUuid("product_id", productDto.getProductId())
                                    .setInt("quantity", quantity)
                                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));

                            session.execute(preparedStatementUpdate.bind()
                                    .setLong("total_quantity", quantity)
                                    .setUuid("category_id", randomCategory.getCategoryId())
                                    .setUuid("store_id", storeDto.getStoreId())
                                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                        })
                )
        ).invoke();

        forkJoinPool.shutdown();
    }
    public void insertStoreProductData(List<StoreDto> storeData, List<ProductDto> productData, List<CategoryDto> categoryData) {
        // Prepare the statement once
        String insertQuery = String.format(
                "INSERT INTO %s.%s (category_id, store_id, product_id, quantity) VALUES (?, ?, ?, ?)",
                keyspaceName, storeProductTable);
        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_id = ? AND store_id = ?",
                keyspaceName, totalProductTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        PreparedStatement preparedStatementUpdate = session.prepare(updateTotalQuery);
// session.execute(session.prepare(insertDataQuery).bind()
//                        .setString("category_name", category)
//                        .setString("store_address", storeAddress)
//                        .setUuid("product_id", productId)
//                        .setInt("quantity", quantity));
        storeData.forEach(storeDto -> productData.forEach(productDto -> {
            CategoryDto randomCategory = getRandomElement(categoryData);
            int quantity = new Random().nextInt(100);

            session.execute(preparedStatement.bind()
                    .setUuid("category_id", randomCategory.getCategoryId())
                    .setUuid("store_id", storeDto.getStoreId())
                    .setUuid("product_id", productDto.getProductId())
                    .setInt("quantity", quantity)
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
            session.execute(preparedStatementUpdate.bind()
                    .setLong("total_quantity", quantity)
                    .setUuid("category_id", randomCategory.getCategoryId())
                    .setUuid("store_id", storeDto.getStoreId())
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
        }));
    }

    public void insertCategoryData(CqlSession session, List<CategoryDto> categoryData) {
        // Assuming your CategoryDto has appropriate getters for each field
        String insertQuery = String.format(
                "INSERT INTO %s.%s (category_id, category_name) VALUES (?, ?)",
                keyspaceName, categoryTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        categoryData.forEach(dto -> session.execute(preparedStatement.bind(
                dto.getCategoryId(),
                dto.getCategoryName()
        ).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)));
    }

    public void insertStoreData(CqlSession session, List<StoreDto> storeData) {
        // Assuming your StoreDto has appropriate getters for each field
        String insertQuery = String.format(
                "INSERT INTO %s.%s (store_id, store_address) VALUES (?, ?)",
                keyspaceName, storeTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        storeData.forEach(dto -> session.execute(preparedStatement.bind(
                dto.getStoreId(),
                dto.getLocation()
        ).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)));
    }

    public void insertProductData(CqlSession session, List<ProductDto> productData) {

        String insertQuery = String.format(
                "INSERT INTO %s.%s (product_id, product_name) VALUES (?, ?)",
                keyspaceName, productTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors()); // Create a ForkJoinPool
        forkJoinPool.submit(() ->
        productData.forEach(dto -> session.execute(preparedStatement.bind(
                dto.getProductId(),
                dto.getName()
        ).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)))
        ).invoke();

        forkJoinPool.shutdown();
    }

    private <T> T getRandomElement(List<T> list) {
        Random rand = new Random();
        return list.get(rand.nextInt(list.size()));
    }


}
