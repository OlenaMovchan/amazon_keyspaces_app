package com.shpp;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;

public class Repository {
    private static final Logger LOGGER = LoggerFactory.getLogger(Repository.class);
    private static final String KEYSPACE_NAME = "my_keyspace";
    private static final String TABLE_NAME = "store_product_table";
    private static final String TABLE_NAME2 = "total_products_by_store";

    static void createTable(CqlSession session) {
        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s ("
                        + "category_name TEXT, "
                        + "store_address TEXT, "
                        + "product_id UUID, "
                        + "quantity INT, "
                        + "PRIMARY KEY ((category_name), store_address, product_id))",
                KEYSPACE_NAME, TABLE_NAME);
        session.execute(createTableQuery);

        String createTableQuery2 = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s ("
                        + "category_name TEXT, "
                        + "store_address TEXT, "
                        + "total_quantity COUNTER, "
                        + "PRIMARY KEY (category_name, store_address))",

                KEYSPACE_NAME, TABLE_NAME2);
        session.execute(createTableQuery2);
        LOGGER.info("Table created successfully");
    }
    static String cat = "";

    //    public static void insertData(CqlSession session) {
    //        // ... (existing code)
    //
    //        String insertDataQuery = String.format(
    //                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
    //                KEYSPACE_NAME, TABLE_NAME);
    //
    //        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    //
    //        for (int j = 1; j <= totalCategories; j++) {
    //            // ... (existing code)
    //
    //            for (int k = 1; k <= totalStores; k++) {
    //                for (int l = 1; l <= 40000; l++) {
    //                    // ... (existing code)
    //
    //                    // Batch insert
    //                    batchBuilder.addStatement(session.prepare(insertDataQuery).bind()
    //                            .setString("category_name", category_name)
    //                            .setString("store_address", store_address)
    //                            .setUuid("product_id", productId)
    //                            .setInt("quantity", quantity));
    //                }
    //            }
    //        }
    //
    //        // Execute batch
    //        session.execute(batchBuilder.build());
    //        LOGGER.info("Data inserted successfully");
    //    }
    //
    //    static void validateAndInsertData(CqlSession session, String category_name, String store_address, UUID productId, int quantity, int total_quantity) {
    //        // ... (existing code)
    //
    //        // Batch update
    //        String updateTotalQuery = String.format(
    //                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_name = ? AND store_address = ?",
    //                KEYSPACE_NAME, TABLE_NAME2);
    //        batchBuilder.addStatement(session.prepare(updateTotalQuery).bind()
    //                .setLong("total_quantity", quantity)
    //                .setString("category_name", category_name)
    //                .setString("store_address", store_address));
    //    }
    public static void insertData(CqlSession session) {
        int totalProducts = 40000;
        int totalStores = 75;
        int totalCategories = 1000;
        Random random = new Random();
        String[] store_address = new String[75];
        String[] categories = new String[1000];
        Faker faker = new Faker(new Locale("uk"));
        for (int i = 0; i < 75; i++) {
            store_address[i] = String.valueOf(faker.address().fullAddress());
        }
        for (int i = 0; i <1000; i++) {
            categories[i] = String.valueOf(faker.commerce().department());
        }
        cat = categories[10];
        String insertDataQuery = String.format(
                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                KEYSPACE_NAME, TABLE_NAME);

        String updateTotalQuery = String.format(
                "UPDATE %s.%s SET total_quantity = total_quantity + ? WHERE category_name = ? AND store_address = ?",
                KEYSPACE_NAME, TABLE_NAME2);
        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        BatchStatementBuilder batchBuilder2 = BatchStatement.builder(DefaultBatchType.UNLOGGED);
       // for (int j = 1; j <= totalCategories; j++) {
            //UUID storeId = UUID.randomUUID();
            //UUID s = UUIDs.random();//

            for (int k = 1; k <= totalStores; k++) {
                for (int l = 1; l <= 40000; l++) {
                    UUID productId = UUID.randomUUID();
                    int quantity = random.nextInt(999);
                    batchBuilder.addStatement(session.prepare(insertDataQuery).bind()
                                                        .setString("category_name", categories[quantity])
                                                       .setString("store_address", store_address[k-1])
                                                        .setUuid("product_id", productId)
                                                        .setInt("quantity", quantity));
                    batchBuilder2.addStatement(session.prepare(updateTotalQuery).bind()
                            .setString("category_name", categories[quantity])
                            .setString("store_address", store_address[k-1])
                            .setLong("total_quantity", quantity));
                    //validateAndInsertData(session, categories[j-1], store_address[k-1], productId, quantity, 0);
                    if (batchBuilder.getStatementsCount() >= 30) {
                        session.execute(batchBuilder.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                        session.execute(batchBuilder2.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
                        batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
                        batchBuilder2 = BatchStatement.builder(DefaultBatchType.UNLOGGED);
                    }
                }
                System.out.println(">>>>>>>>40000");
                //session.execute(batchBuilder.build().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE));
//                if (!batchBuilder.) {
//                    session.execute(batchBuilder.build());
//                }
            }
        //}
        LOGGER.info("Data inserted successfully");
    }

    static void validateAndInsertData(CqlSession session, String category_name, String store_address, UUID productId, int quantity, int total_quantity) {

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

    static void selectData(CqlSession session) {
        String selectDataQuery = String.format(
                "SELECT * FROM \"%s\".\"%s\" WHERE category_name = '"+cat+"';",
                KEYSPACE_NAME, TABLE_NAME2);

        ResultSet resultSet = session.execute(selectDataQuery);

        ArrayList<Long> total = new ArrayList<>();
        String Store_address = null;
        String Category_name = null;
        long max = 0;
        for (Row row : resultSet) {
            Long Total_quantity = row.getLong("total_quantity");
            if (Total_quantity > max) {
                max = Total_quantity;
                Store_address = row.getString("store_address");
                Category_name = row.getString("category_name");
            }
            total.add(Total_quantity);
        }
        LOGGER.info("Category_name: {}, Store_address: {}, Total_Quantity: {}",
                Category_name, Store_address, max);
    }
}
