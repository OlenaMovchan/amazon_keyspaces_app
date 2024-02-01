package com.shpp.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
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

public class DataInsertion {
    private final Logger LOGGER = LoggerFactory.getLogger(DataInsertion.class);
    private final CqlSession session;
    private int maxRetries = 3;
    private long initialDelayMillis = 1000;

    public DataInsertion(CqlSession session) {
        this.session = session;
    }

    public void insertCategoryData(CqlSession session, List<CategoryDto> categoryData, String keyspaceName, String categoryTable) throws Exception {
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
                    LOGGER.warn("Table CategoryDto. Retrying failed insertions on attempt " + attempt);
                    categoryData = new ArrayList<>(failedInsertions);
                    failedInsertions.clear();
                }
            } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                handleRetry(attempt, maxRetries, initialDelayMillis, ex);
            }
        }
    }

    public void handleRetry(int attempt, int maxRetries, long initialDelayMillis, Exception ex) throws Exception {
        LOGGER.warn("Query timed out on attempt " + attempt);
        if (attempt == maxRetries) {
            LOGGER.error("Max retries reached. Exiting.");
            throw ex;
        }
        long delayMillis = initialDelayMillis * attempt;
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void insertStoreData(CqlSession session, List<StoreDto> storeData, String keyspaceName, String storeTable) throws Exception {
        String insertQuery = String.format(
                "INSERT INTO %s.%s (store_id, store_address) VALUES (?, ?)",
                keyspaceName, storeTable);
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        List<StoreDto> failedRecords = new ArrayList<>();
        for (int attempt = 1; attempt <= 3; attempt++) {
            failedRecords.clear();
            for (StoreDto storeDto : storeData) {
                try {
                    session.execute(preparedStatement.bind(storeDto.getStoreId(),
                            storeDto.getLocation()).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                    failedRecords.add(storeDto);
                }
            }
            if (failedRecords.isEmpty()) {
                break;
            } else {
                LOGGER.warn("Table StoreDto. Retrying failed insertions on attempt " + attempt);
                Thread.sleep(1000);
                storeData = new ArrayList<>(failedRecords);
            }
        }
    }

    public void insertProductData(CqlSession session, List<ProductDto> productData, String keyspaceName, String productTable) throws Exception {

        String insertQuery = String.format(
                "INSERT INTO %s.%s (product_id, product_name, category_id) VALUES (?, ?, ?)",
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
                                        dto.getName(),
                                        dto.getCategoryId()
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
                    LOGGER.warn("Table ProductDto. Retrying failed insertions on attempt " + attempt);
                    productData = new ArrayList<>(failedInsertions);
                    failedInsertions.clear();
                }
            } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                handleRetry(attempt, maxRetries, initialDelayMillis, ex);
            }
        }
    }

    public void insertStoreProductData(List<StoreDto> storeData, List<ProductDto> productData, String keyspaceName, String storeProductTable, String totalProductTable) throws Exception {
        String insertQuery = String.format(
                "INSERT INTO %s.%s (store_id, product_id, quantity) VALUES (?, ?, ?)",//category_id,   ?,
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
                ForkJoinPool forkJoinPool = new ForkJoinPool(10);
                List<StoreDto> finalStoreData = storeData;
                List<ProductDto> finalProductData = productData;
                forkJoinPool.submit(() -> processStoreProductData(finalStoreData, finalProductData, preparedStatement, preparedStatementUpdate, failedStores, failedProducts)).invoke();
                forkJoinPool.shutdown();

                if (failedStores.isEmpty()) {
                    break;
                } else {
                    LOGGER.error("Table store_product. Retrying failed insertions on attempt " + attempt);
                    storeData = new ArrayList<>(failedStores);
                    productData = new ArrayList<>(failedProducts);
                    failedStores.clear();
                    failedProducts.clear();
                }
            } catch (DriverTimeoutException | WriteFailureException | WriteTimeoutException ex) {
                handleRetry(attempt, maxRetries, initialDelayMillis, ex);
            }
        }
    }

    public void processStoreProductData(List<StoreDto> storeData, List<ProductDto> productData,
                                        PreparedStatement preparedStatement, PreparedStatement preparedStatementUpdate,
                                        List<StoreDto> failedStores, List<ProductDto> failedProducts) {
        storeData.parallelStream().forEach(storeDto ->
                productData.parallelStream().forEach(productDto -> {
                    int quantity = new Random().nextInt(100);
                    try {
                        session.execute(preparedStatement.bind()
                                //.setUuid("category_id", productDto.getCategoryId())
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
}
