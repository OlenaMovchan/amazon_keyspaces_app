package com.shpp.repository;

import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.shpp.dto.CategoryDto;
import com.shpp.dto.ProductDto;
import com.shpp.dto.StoreDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;

public class DataIns {
    private final Logger LOGGER = LoggerFactory.getLogger(DataIns.class);
    private final CqlSession session;
    private final String keyspaceName;
    private final String storeProductTable;
    private final String totalProductTable;
    private String categoryTable;
    private String storeTable;
    private String productTable;
    int maxRetries = 3;
    long initialDelayMillis = 1000;
    long maxDelayMillis = 30000;
    DataGenerator dataGenerator = new DataGenerator();


    public DataIns(CqlSession session, String keyspaceName, String storeProductTable, String totalProductTable, String productTable, String storeTable, String categoryTable) {
        this.session = session;
        this.keyspaceName = keyspaceName;
        this.storeProductTable = storeProductTable;
        this.totalProductTable = totalProductTable;
        this.productTable = productTable;
        this.storeTable = storeTable;
        this.categoryTable = categoryTable;
    }
    public void insertStoreProductDataParallelWithTryNew(List<StoreDto> storeData, List<ProductDto> productData, List<CategoryDto> categoryData) throws Exception {
        String insertQuery = String.format(
                "INSERT INTO %s.%s (category_id, store_id, product_id, quantity) VALUES (?, ?, ?, ?)",
                keyspaceName, storeProductTable);
        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_id = ? AND store_id = ?",
                keyspaceName, totalProductTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        PreparedStatement preparedStatementUpdate = session.prepare(updateTotalQuery);

        List<StoreDto> failedStores = new ArrayList<>();
        List<ProductDto> failedProducts = new ArrayList<>();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
                List<StoreDto> finalStoreData = storeData;
                List<ProductDto> finalProductData = productData;
                forkJoinPool.submit(() -> processStoreProductData(finalStoreData, finalProductData, preparedStatement, preparedStatementUpdate, failedStores, failedProducts)).invoke();
                forkJoinPool.shutdown();

                // Check if there are no failed insertions
                if (failedStores.isEmpty()) {
                    break;
                } else {
                    System.err.println("Store_product. Retrying failed insertions on attempt " + attempt);
                    storeData = new ArrayList<>(failedStores);
                    productData = new ArrayList<>(failedProducts);
                    failedStores.clear();
                    failedProducts.clear();
                }
            } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                handleRetry(attempt, maxRetries, initialDelayMillis, maxDelayMillis, ex);
            }
        }
    }

    // Method to process store-product data in parallel
    public void processStoreProductData(List<StoreDto> storeData, List<ProductDto> productData,
                                         PreparedStatement preparedStatement, PreparedStatement preparedStatementUpdate,
                                         List<StoreDto> failedStores, List<ProductDto> failedProducts) {
        storeData.parallelStream().forEach(storeDto ->
                productData.parallelStream().forEach(productDto -> {
                    int quantity = new Random().nextInt(100);
                    try {
                        session.execute(preparedStatement.bind()
                                .setUuid("category_id", productDto.getCategoryId())
                                .setUuid("store_id", storeDto.getStoreId())
                                .setUuid("product_id", productDto.getProductId())
                                .setInt("quantity", quantity)
                                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));

                        session.execute(preparedStatementUpdate.bind()
                                .setLong("total_quantity", quantity)
                                .setUuid("category_id", productDto.getCategoryId())
                                .setUuid("store_id", storeDto.getStoreId())
                                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                    } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                        failedStores.add(storeDto);
                        failedProducts.add(productDto);
                    }
                }));
    }

    public void insertStoreProductDataParallelWithTry(List<StoreDto> storeData, List<ProductDto> productData, List<CategoryDto> categoryData) throws Exception {
        String insertQuery = String.format(
                "INSERT INTO %s.%s (category_id, store_id, product_id, quantity) VALUES (?, ?, ?, ?)",
                keyspaceName, storeProductTable);
        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_id = ? AND store_id = ?",
                keyspaceName, totalProductTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        PreparedStatement preparedStatementUpdate = session.prepare(updateTotalQuery);

        List<StoreDto> failedStores = new ArrayList<>();
        List<ProductDto> failedProducts = new ArrayList<>();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
                List<StoreDto> finalStoreData = storeData;
                List<ProductDto> finalProductData = productData;
                forkJoinPool.submit(() ->
                        finalStoreData.parallelStream().forEach(storeDto ->
                                finalProductData.parallelStream().forEach(productDto -> {
                                    int quantity = new Random().nextInt(100);
                                    try {
                                        session.execute(preparedStatement.bind()
                                                .setUuid("category_id", productDto.getCategoryId())
                                                .setUuid("store_id", storeDto.getStoreId())
                                                .setUuid("product_id", productDto.getProductId())
                                                .setInt("quantity", quantity)
                                                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));

                                        session.execute(preparedStatementUpdate.bind()
                                                .setLong("total_quantity", quantity)
                                                .setUuid("category_id", productDto.getCategoryId())
                                                .setUuid("store_id", storeDto.getStoreId())
                                                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                                    } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                                        failedStores.add(storeDto);
                                        failedProducts.add(productDto);
                                    }
                                })
                        )
                ).invoke();
                forkJoinPool.shutdown();

                if (failedStores.isEmpty()) {
                    break;
                } else {
                    System.err.println("Store_product. Retrying failed insertions on attempt " + attempt);
                    storeData = new ArrayList<>(failedStores);
                    productData = new ArrayList<>(failedProducts);
                    failedStores.clear();
                    failedProducts.clear();
                }
            } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                handleRetry(attempt, maxRetries, initialDelayMillis, maxDelayMillis, ex);
            }
        }
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

        ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
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

        String insertQuery = String.format(
                "INSERT INTO %s.%s (category_id, store_id, product_id, quantity) VALUES (?, ?, ?, ?)",
                keyspaceName, storeProductTable);
        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_id = ? AND store_id = ?",
                keyspaceName, totalProductTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        PreparedStatement preparedStatementUpdate = session.prepare(updateTotalQuery);
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

    public void insertCategoryDataWithTry(CqlSession session, List<CategoryDto> categoryData) throws Exception {
        String insertQuery = String.format(
                "INSERT INTO %s.%s (category_id, category_name) VALUES (?, ?)",
                keyspaceName, categoryTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        List<CategoryDto> failedInsertions = new ArrayList<>();
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                categoryData.forEach(dto -> session.execute(preparedStatement.bind(
                        dto.getCategoryId(),
                        dto.getCategoryName()
                ).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)));
                if (failedInsertions.isEmpty()) {
                    break;
                } else {
                    System.err.println("Category. Retrying failed insertions on attempt " + attempt);
                    categoryData = new ArrayList<>(failedInsertions);
                    failedInsertions.clear();
                }
            } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
               handleRetry(attempt, maxRetries, initialDelayMillis, maxDelayMillis, ex);
            }
        }
    }
    public void handleRetry(int attempt, int maxRetries, long initialDelayMillis, long maxDelayMillis, Exception ex) throws Exception {
        System.err.println("Query timed out on attempt " + attempt);
        if (attempt == maxRetries) {
            System.err.println("Max retries reached. Exiting.");
            throw ex;
        }
        long delayMillis = Math.min(initialDelayMillis * (1 << attempt), maxDelayMillis);
        try {
            Thread.sleep(delayMillis); // Apply exponential backoff
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public void insertStoreDataWithTry(CqlSession session, List<StoreDto> storeData) throws Exception {

        String insertQuery = String.format(
                "INSERT INTO %s.%s (store_id, store_address) VALUES (?, ?)",
                keyspaceName, storeTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        List<StoreDto> failedInsertions = new ArrayList<>();
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                storeData.forEach(dto -> session.execute(preparedStatement.bind(
                        dto.getStoreId(),
                        dto.getLocation()
                ).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)));
                if (failedInsertions.isEmpty()) {
                    break;
                } else {
                    System.err.println("Store. Retrying failed insertions on attempt " + attempt);
                    storeData = new ArrayList<>(failedInsertions);
                    failedInsertions.clear();
                }
            } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                handleRetry(attempt, maxRetries, initialDelayMillis, maxDelayMillis, ex);
            }
        }
    }

    public void insertProductDataWithTry(CqlSession session, List<ProductDto> productData) throws Exception {

        String insertQuery = String.format(
                "INSERT INTO %s.%s (product_id, product_name) VALUES (?, ?)",
                keyspaceName, productTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        List<ProductDto> failedInsertions = new ArrayList<>();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
                List<ProductDto> finalProductData = productData;
                forkJoinPool.submit(() ->
                        finalProductData.forEach(dto -> {
                            try {
                                session.execute(preparedStatement.bind(
                                        dto.getProductId(),
                                        dto.getName()
                                ).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                            } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                                failedInsertions.add(dto);
                            }
                        })
                ).invoke();
                forkJoinPool.shutdown();

                if (failedInsertions.isEmpty()) {
                    break;
                } else {
                    LOGGER.warn("Product. Retrying failed insertions on attempt " + attempt);
                    productData = new ArrayList<>(failedInsertions);
                    failedInsertions.clear();
                }
            } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                handleRetry(attempt, maxRetries, initialDelayMillis, maxDelayMillis, ex);
            }
        }
    }

    public void insertProductData2(CqlSession session, List<ProductDto> productData) {
        int maxRetries = 3;
        long initialDelayMillis = 1000; // 1 second
        long maxDelayMillis = 30000; // 30 seconds
        String insertQuery = String.format(
                "INSERT INTO %s.%s (product_id, product_name) VALUES (?, ?)",
                keyspaceName, productTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
                forkJoinPool.submit(() ->
                        productData.forEach(dto -> session.execute(preparedStatement.bind(
                                dto.getProductId(),
                                dto.getName()
                        ).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)))
                ).invoke();
                forkJoinPool.shutdown();
                break; // Break out of the retry loop if successful
            } catch (DriverTimeoutException ex) {
                System.err.println("Query timed out on attempt " + attempt);
                if (attempt == maxRetries) {
                    System.err.println("Max retries reached. Exiting.");
                    throw ex; // Throw the exception if max retries are reached
                }
                long delayMillis = Math.min(initialDelayMillis * (1 << attempt), maxDelayMillis);
                try {
                    Thread.sleep(delayMillis); // Apply exponential backoff
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
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
