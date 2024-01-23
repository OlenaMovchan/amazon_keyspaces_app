package com.shpp.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.github.javafaker.Faker;
import com.shpp.dto.Store;
import jakarta.validation.ConstraintViolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class DataInsertion {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataInsertion.class);
    private static final String KEYSPACE_NAME = "my_keyspace";
    private static final String TABLE_NAME = "store_product_table";
    private static final String TABLE_NAME2 = "total_products_by_store";
    private static final int BATCH_SIZE_LIMIT = 30;
    private static CqlSession session;

    public DataInsertion(CqlSession session) {
        this.session = session;
    }

    public static void insertData(int totalStores, int totalProducts, int totalCategories) {
        String[] categories = generateCategories(totalCategories);

        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        BatchStatementBuilder batchBuilderUpdate = BatchStatement.builder(DefaultBatchType.UNLOGGED);

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        IntStream.range(0, totalStores)
                .forEach(k -> executorService.execute(() -> generateAndValidateData(
                        categories, totalProducts, batchBuilder, batchBuilderUpdate)));

        executorService.shutdown();

        executeRemainingStatements(session, batchBuilder, batchBuilderUpdate);
        LOGGER.info("Data inserted successfully");
    }

    private static void executeRemainingStatements(
            CqlSession session, BatchStatementBuilder batchBuilder, BatchStatementBuilder batchBuilderUp) {
        if (batchBuilder.getStatementsCount() > 0) {
            session.execute(batchBuilder.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
            session.execute(batchBuilderUp.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
        }
    }

    private static void generateAndValidateData(
            String[] categories, int totalProducts,
            BatchStatementBuilder batchBuilder, BatchStatementBuilder batchBuilderUp) {
        Random random = new Random();
        String category = categories[10];
        String insertDataQuery = String.format(
                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                KEYSPACE_NAME, TABLE_NAME);
        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_name = ? AND store_address = ?",
                KEYSPACE_NAME, TABLE_NAME2);

//        for (int l = 1; l <= totalProducts; l++) {
//            Store storeDTO = generateStoreDTO(75);
//            Set<ConstraintViolation<Store>> violations = ValidatorClass.validateStoreDTO(storeDTO);

//            if (violations.isEmpty()) {
//                batchBuilder.addStatement(session.prepare(insertDataQuery).bind()
//                        .setString("category_name", storeDTO.getCategoryName())
//                        .setString("store_address", storeDTO.getLocation())
//                        .setUuid("product_id", UUID.randomUUID())
//                        .setInt("quantity", random.nextInt(25)));
//
//                batchBuilderUp.addStatement(session.prepare(updateTotalQuery).bind()
//                        .setString("category_name", storeDTO.getCategoryName())
//                        .setString("store_address", storeDTO.getLocation())
//                        .setLong("total_quantity", random.nextInt(25)));
//            } else {
//                LOGGER.warn("Validation failed for StoreDTO: {}", storeDTO);
//            }

            if (batchBuilder.getStatementsCount() >= BATCH_SIZE_LIMIT) {
                session.execute(batchBuilder.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                session.execute(batchBuilderUp.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
                batchBuilderUp = BatchStatement.builder(DefaultBatchType.UNLOGGED);
            }
        //}
    }

    private static String[] generateCategories(int totalCategories) {
        return IntStream.range(0, totalCategories)
                .mapToObj(i -> new Faker(new Locale("uk")).commerce().department())
                .toArray(String[]::new);
    }

    private static String[] generateStoreDTO(int totalStores) {
        return IntStream.range(0, totalStores)
                .mapToObj(i -> new Faker(new Locale("uk")).address().fullAddress())
                .toArray(String[]::new);
    }

}

