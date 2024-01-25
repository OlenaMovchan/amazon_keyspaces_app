package com.shpp.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.javafaker.Faker;
import com.shpp.ValidatorClass;
import com.shpp.dto.CategoryDto;
import com.shpp.dto.StoreDto;
import jakarta.validation.ConstraintViolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;

import com.shpp.dto.ProductDto;


import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataInsertion {
    ValidatorClass validatorClass = new ValidatorClass<>();
    private final Logger LOGGER = LoggerFactory.getLogger(DataInsertion.class);
    private final String KEYSPACE_NAME = "my_keyspace";
    private final String TABLE_NAME = "store_product_table_";
    private final String TABLE_NAME2 = "total_products_by_store_";
    public int totalProducts = 400;
    public int totalStores = 5;
    public int totalCategories = 100;
    public int numberOfThreads =2;
    public String category = "";
    public String getCategory() {
        return category;
    }

    public void insertData(CqlSession session) {
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        Random random = new Random();
        String[] storeAddress = generateStoreData(totalStores);
        String[] categories = generateCategoryData(totalCategories);

        category = categories[1];

        try {
            IntStream.range(0, totalStores)
                    //.parallel()
                    .forEach(store -> insertStoreData(store, session, categories, storeAddress, random, executorService));
        } finally {
            executorService.shutdown();
        }

        LOGGER.info("Data inserted successfully");
    }

    public void insertStoreData(int i, CqlSession session, String[] categories, String[] storeAddress, Random random, ExecutorService executorService) {
        IntStream.range(0, totalProducts)
                //.parallel()
                .forEach(numProduct -> {
                    ProductDto product = new ProductDto(UUID.randomUUID());
                    int quantity = random.nextInt(totalCategories);
                    try {
                        executeInsert(session, categories[quantity], storeAddress[i], product.getProductId(), quantity, executorService);
                    } catch (Exception e) {
                        LOGGER.error("Error inserting data for store {}, product {}: {}", storeAddress[i], product.getProductId(), e.getMessage());
                    }
                });

        LOGGER.info("Data insert in " + i + " store, address: " + storeAddress[i]);
    }

    public void executeInsert(CqlSession session, String category, String storeAddress, UUID productId, int quantity, ExecutorService executorService) {
        executorService.submit(() -> {
            String insertDataQuery = String.format(
                    "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                    KEYSPACE_NAME, TABLE_NAME);

            String updateTotalQuery = String.format(
                    "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_name = ? AND store_address = ?",
                    KEYSPACE_NAME, TABLE_NAME2);
            try {
                session.execute(session.prepare(insertDataQuery).bind()
                        .setString("category_name", category)
                        .setString("store_address", storeAddress)
                        .setUuid("product_id", productId)
                        .setInt("quantity", quantity));

                session.execute(session.prepare(updateTotalQuery).bind()
                        .setString("category_name", category)
                        .setString("store_address", storeAddress)
                        .setLong("total_quantity", quantity));
            } catch (Exception e) {
                LOGGER.error("Error executing query: {}", e.getMessage());
            }
        });
    }

    public String[] generateStoreData(int totalStore) {
        Faker faker = new Faker(new Locale("uk"));
        return IntStream.range(0, totalStore)
                .mapToObj(i -> faker.address().fullAddress())
                .map(address -> {
                    StoreDto store = new StoreDto(address);
                    Set<ConstraintViolation<StoreDto>> violations = validatorClass.validateDTO(store);
                    return violations.isEmpty() ? address : null;
                })
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }

    public String[] generateCategoryData(int totalCategory) {
        Faker faker = new Faker(new Locale("uk"));

        return IntStream.range(0, totalCategory)
                .mapToObj(i -> faker.commerce().department())
                .map(name -> {
                    CategoryDto category = new CategoryDto(name);
                    Set<ConstraintViolation<CategoryDto>> violations = validatorClass.validateDTO(category);
                    return violations.isEmpty() ? name : null;
                })
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }
}
