package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.github.javafaker.Faker;
import com.shpp.dto.Category;
import com.shpp.dto.Product;
import com.shpp.dto.Store;
import jakarta.validation.ConstraintViolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Repository {
    static ValidatorClass validatorClass = new ValidatorClass<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(Repository.class);
    private static final String KEYSPACE_NAME = "my_keyspace";
    private static final String TABLE_NAME = "store_product_table_";
    private static final String TABLE_NAME2 = "total_products_by_store_";
    static int totalProducts = 100;
    static int totalStores = 5;
    static int totalCategories = 25;
    static int batchSizeLimit = 30;
    static String category = "";
    public  void insertData(CqlSession session) {
        Random random = new Random();
        String[] storeAddress = generateStoreData(totalStores);
        String[] categories = generateCategoryData(totalCategories);

        category = categories[10];
        String insertDataQuery = String.format(
                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                KEYSPACE_NAME, TABLE_NAME);

        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_name = ? AND store_address = ?",
                KEYSPACE_NAME, TABLE_NAME2);

        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        BatchStatementBuilder batchBuilderUpdate = BatchStatement.builder(DefaultBatchType.UNLOGGED);

        for (int i = 1; i <= totalStores; i++) {
            for (int j = 1; j <= totalProducts; j++) {
                Product product = new Product(UUID.randomUUID());
                int quantity = random.nextInt(totalCategories);
                batchBuilder.addStatement(session.prepare(insertDataQuery).bind()
                        .setString("category_name", categories[quantity])
                        .setString("store_address", storeAddress[i-1])
                        .setUuid("product_id", product.getProductId())
                        .setInt("quantity", quantity));
                batchBuilderUpdate.addStatement(session.prepare(updateTotalQuery).bind()
                        .setString("category_name", categories[quantity])
                        .setString("store_address", storeAddress[i-1])
                        .setLong("total_quantity", quantity));
                if (batchBuilder.getStatementsCount() >= batchSizeLimit) {
                    session.execute(batchBuilder.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                    session.execute(batchBuilderUpdate.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                    batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
                    batchBuilderUpdate = BatchStatement.builder(DefaultBatchType.UNLOGGED);
                }
            }
        }
        LOGGER.info("Data inserted successfully");
    }
    public  String[] generateStoreData(int totalStore) {
        Faker faker = new Faker(new Locale("uk"));

        return IntStream.range(0, totalStore)
                .mapToObj(i -> faker.address().fullAddress())
                .map(address -> {
                    Store store = new Store(address);
                    Set<ConstraintViolation<Store>> validated = validatorClass.validateDTO(store);
                    return validated.isEmpty() ? address : null;
                })
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }
    public  String[] generateCategoryData(int totalCategory) {
        Faker faker = new Faker(new Locale("uk"));

        return IntStream.range(0, totalCategory)
                .mapToObj(i -> faker.commerce().department())
                .map(name -> {
                    Category category = new Category(name);
                    Set<ConstraintViolation<Category>> validated = validatorClass.validateDTO(category);
                    return validated.isEmpty() ? name : null;
                })
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }


    public  void insertData2(CqlSession session) {
        Random random = new Random();
        String[] storeAddress = generateStoreData(totalStores);
        String[] categories = generateCategoryData(totalCategories);

        String insertDataQuery = String.format(
                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                KEYSPACE_NAME, TABLE_NAME);

        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_name = ? AND store_address = ?",
                KEYSPACE_NAME, TABLE_NAME2);

//        List<BatchStatement> batches = IntStream.range(1, totalStores + 1)
//                .parallel()
//                .mapToObj(k -> generateBatch(session, categories, storeAddress[k - 1], random, insertDataQuery, updateTotalQuery))
//                .collect(Collectors.toList());
//
//        batches.forEach(batch -> session.execute(batch.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)));
//        LOGGER.info("Data inserted successfully");

        List<BatchStatement> batches = IntStream.range(1, totalStores + 1)
                .parallel()
                .mapToObj(k -> generateBatches(session, categories, storeAddress[k - 1], random, insertDataQuery, updateTotalQuery))
                .flatMap(List::stream)
                .collect(Collectors.toList());

        batches.forEach(batch -> session.execute(batch.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)));
        LOGGER.info("Data inserted successfully");
    }
    private  List<BatchStatement> generateBatches(
            CqlSession session, String[] categories, String storeAddress, Random random,
            String insertDataQuery, String updateTotalQuery) {
        List<BatchStatement> batches = new ArrayList<>();

        for (int l = 1; l <= totalProducts; l += batchSizeLimit) {
            BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);

            for (int i = 0; i < batchSizeLimit && (l + i) <= totalProducts; i++) {
                UUID productId = UUID.randomUUID();
                int quantity = random.nextInt(24);

                batchBuilder.addStatement(session.prepare(insertDataQuery).bind()
                        .setString("category_name", categories[quantity])
                        .setString("store_address", storeAddress)
                        .setUuid("product_id", productId)
                        .setInt("quantity", quantity));

                batchBuilder.addStatement(session.prepare(updateTotalQuery).bind()
                        .setString("category_name", categories[quantity])
                        .setString("store_address", storeAddress)
                        .setLong("total_quantity", quantity));
            }

            batches.add(batchBuilder.build());
        }

        return batches;
    }



    public  void executeInsert(CqlSession session) {
        int totalStores = 5;
        int totalProducts = 100;
        int totalCategories = 25;
        int batchSizeLimit = 30;
        String[] storeAddress = new String[totalStores];
        String[] categories = new String[1000];
        Faker faker = new Faker(new Locale("uk"));
        for (int i = 0; i < totalStores; i++) {
            storeAddress[i] = String.valueOf(faker.address().fullAddress());
        }
        for (int i = 0; i < totalCategories; i++) {
            categories[i] = String.valueOf(faker.commerce().department());
        }
        category = categories[10];
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        BatchStatementBuilder batchBuilderUpdate = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        IntStream.range(0, totalStores)
                .forEachOrdered(store -> executorService.execute(() -> batchInsert(session, store,
                        categories, storeAddress, totalProducts, batchBuilder, batchBuilderUpdate)));

        executorService.shutdown();

        executeRemainingStatements(session, batchBuilder, batchBuilderUpdate);

        LOGGER.info("Data inserted successfully");
    }

      void batchInsert(CqlSession session, int storeNumber, String[] categories, String[] storeAddress, int totalProducts, BatchStatementBuilder batchBuilder, BatchStatementBuilder batchBuilderUpdate) {
        Random random = new Random();
        String insertDataQuery = String.format(
                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                KEYSPACE_NAME, TABLE_NAME);

        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_name = ? AND store_address = ?",
                KEYSPACE_NAME, TABLE_NAME2);
        for (int l = 1; l <= totalProducts; l++) {
            UUID productId = UUID.randomUUID();
            int quantity = random.nextInt(24);
            batchBuilder.addStatement(session.prepare(insertDataQuery).bind()
                    .setString("category_name", categories[quantity])
                    .setString("store_address", storeAddress[storeNumber])
                    .setUuid("product_id", productId)
                    .setInt("quantity", quantity));
            batchBuilderUpdate.addStatement(session.prepare(updateTotalQuery).bind()
                    .setString("category_name", categories[quantity])
                    .setString("store_address", storeAddress[storeNumber])
                    .setLong("total_quantity", quantity));
            if (batchBuilder.getStatementsCount() >= batchSizeLimit) {
                executeBatch(session, batchBuilder, batchBuilderUpdate);
            }
        }
    }
public void addStatment(CqlSession session, BatchStatementBuilder batchBuilder, BatchStatementBuilder batchBuilderUpdate){
//    batchBuilder.addStatement(session.prepare(insertDataQuery).bind()
//            .setString("category_name", categories[quantity])
//            .setString("store_address", storeAddress[storeNumber])
//            .setUuid("product_id", productId)
//            .setInt("quantity", quantity));
//    batchBuilderUpdate.addStatement(session.prepare(updateTotalQuery).bind()
//            .setString("category_name", categories[quantity])
//            .setString("store_address", storeAddress[storeNumber])
//            .setLong("total_quantity", quantity));
}
    private  void executeRemainingStatements(
            CqlSession session, BatchStatementBuilder batchBuilder, BatchStatementBuilder batchBuilderUpdate) {
        if (batchBuilder.getStatementsCount() > 0) {
            session.execute(batchBuilder.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
            session.execute(batchBuilderUpdate.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
        }
    }

    void validateAndInsertData(CqlSession session, String category_name, String store_address, UUID productId, int quantity, int total_quantity) {

        System.out.println("Validating and inserting data: "
                + "CategoryName=" + category_name + ", "
                + "StoreAddress=" + store_address + ", "
                + "ProductID=" + productId + ", "
                + "Quantity=" + quantity);

        String insertDataQuery = String.format(
                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                KEYSPACE_NAME, TABLE_NAME);
        BoundStatement statement = session.prepare(insertDataQuery).bind()
                .setString("category_name", category_name)
                .setString("store_address", store_address)
                .setUuid("product_id", productId)
                .setInt("quantity", quantity);
        session.execute(statement.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));

        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_name = ? AND store_address = ?",
                KEYSPACE_NAME, TABLE_NAME2);
        statement = session.prepare(updateTotalQuery).bind()
                .setLong("total_quantity", quantity)
                .setString("category_name", category_name)
                .setString("store_address", store_address);
        session.execute(statement.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
    }

    public BoundStatement createInsertStatement(CqlSession session, String categoryName, String storeAddress, UUID productId, int quantity) {
        String insertDataQuery = String.format(
                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                KEYSPACE_NAME, TABLE_NAME);

        return session.prepare(insertDataQuery).bind()
                .setString("category_name", categoryName)
                .setString("store_address", storeAddress)
                .setUuid("product_id", productId)
                .setInt("quantity", quantity);
    }

    public  void executeBatch(CqlSession session, BatchStatementBuilder batchBuilder, BatchStatementBuilder batchBuilderUpdate) {
        session.execute(batchBuilder.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
        session.execute(batchBuilderUpdate.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
        batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        batchBuilderUpdate = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    }

}
