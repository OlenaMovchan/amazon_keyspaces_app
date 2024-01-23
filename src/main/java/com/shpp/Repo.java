package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.javafaker.Faker;
import com.shpp.dto.Category;
import com.shpp.dto.Product;
import com.shpp.dto.Store;
import jakarta.validation.ConstraintViolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class Repo {

    static ValidatorClass validatorClass = new ValidatorClass<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(Repository.class);
    private static final String KEYSPACE_NAME = "my_keyspace";
    private static final String TABLE_NAME = "store_product_table_";
    private static final String TABLE_NAME2 = "total_products_by_store_";
    static int totalProducts = 10000;
    static int totalStores = 75;
    static int totalCategories = 1000;
    static String category = "";
    static int numberOfThreads = 100;

    public void insertData(CqlSession session) {

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);




        Random random = new Random();
        String[] storeAddress = generateStoreData(totalStores);
        String[] categories = generateCategoryData(totalCategories);

        category = categories[10];
        try {
            CompletableFuture<Void>[] futures = IntStream.range(1, totalStores+1)
                    .parallel()
                    .mapToObj(i -> CompletableFuture.runAsync(() -> insertStoreData(i, session, categories, storeAddress, random), executorService))
                    .toArray(CompletableFuture[]::new);

            // Wait for all CompletableFuture tasks to complete
            CompletableFuture.allOf(futures).join();
        } finally {
            executorService.shutdown();
        }
//        for (int i = 1; i <= totalStores; i++) {
//            for (int j = 1; j <= totalProducts; j++) {
//                Product product = new Product(UUID.randomUUID());
//                int quantity = random.nextInt(totalCategories);
//
//                executeInsert(session, categories[quantity], storeAddress[i - 1], product.getProductId(), quantity);
//            }
//            LOGGER.info("Data insert in" + i + "store");
//        }

        LOGGER.info("Data inserted successfully");
    }
    private void insertStoreData(int i, CqlSession session, String[] categories, String[] storeAddress, Random random) {
        for (int j = 1; j <= totalProducts; j++) {
            Product product = new Product(UUID.randomUUID());
            int quantity = random.nextInt(totalCategories);

            executeInsert(session, categories[quantity], storeAddress[i - 1], product.getProductId(), quantity);
        }
        LOGGER.info("Data insert in" + i + "store");
    }
    private void executeInsert(CqlSession session, String category, String storeAddress, UUID productId, int quantity) {
        String insertDataQuery = String.format(
                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                KEYSPACE_NAME, TABLE_NAME);

        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_name = ? AND store_address = ?",
                KEYSPACE_NAME, TABLE_NAME2);

        session.executeAsync(session.prepare(insertDataQuery).bind()
                        .setString("category_name", category)
                        .setString("store_address", storeAddress)
                        .setUuid("product_id", productId)
                        .setInt("quantity", quantity))
                .thenApply(rs -> null);

        session.executeAsync(session.prepare(updateTotalQuery).bind()
                        .setString("category_name", category)
                        .setString("store_address", storeAddress)
                        .setLong("total_quantity", quantity))
                .thenApply(rs -> null);
    }

    public String[] generateStoreData(int totalStore) {
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

    public String[] generateCategoryData(int totalCategory) {
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


}
