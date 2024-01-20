package com.shpp;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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
    //CREATE TABLE IF NOT EXISTS keyspace_name.store_product_table (
//    category_name TEXT,
//    store_address TEXT,
//    product_id UUID,
//    quantity INT,
//    PRIMARY KEY ((category_name, store_address), product_id)
//);
//
//CREATE TABLE IF NOT EXISTS keyspace_name.total_products_by_store (
//    category_name TEXT,
//    store_address TEXT,
//    total_quantity INT,
//    PRIMARY KEY (category_name, store_address)
//);
    static void createTable(CqlSession session) {
        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s ("
                        + "category_name TEXT, "
                        + "store_address TEXT, "
                        + "product_id UUID, "
                        + "quantity INT, "
                        + "PRIMARY KEY ((category_name), store_address, product_id))",
                //+ "WITH CLUSTERING ORDER BY (quantity DESC)",
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
    //+ "WITH CLUSTERING ORDER BY (total_quantity DESC)",
    public static void insertData(CqlSession session) {
        int totalProducts = 40000;
        int totalStores = 75;
        int totalCategories = 1000;
        Random random = new Random();
        String[] store_address = new String[5];
        String[] categories = new String[20];
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
//        BoundStatement boundStatement = session.prepare(insertDataQuery).bind()
//                .setUuid("id", java.util.UUID.randomUUID())
//                .setString("name", "Product")
//                .setString("description", "description");
        for (int j = 1; j <= totalCategories; j++) {
            UUID storeId = UUID.randomUUID();
            UUID s = UUIDs.random();//

            for (int k = 1; k <= totalStores; k++) {
                //Adjust the logic for quantity as needed
                for (int l = 1; l <= 40000; l++) {
                    System.out.println(faker.commerce().department());
                    System.out.println(faker.address().fullAddress());
                    UUID productId = UUID.randomUUID();
                    int quantity = random.nextInt(1000);
                    validateAndInsertData(session, categories[j-1], store_address[k-1], productId, quantity, 0);
                }

            }
        }
//        for (int i = 0; i < 75; i++) {
//            BoundStatement boundStatement = session.prepare(insertDataQuery).bind()
//                    .setUuid("category_id", UUID.randomUUID())
//                    .setString("name", "Product" + i)
//                    .setString("description", "Description" + i);
//
//            //boundStatement.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
//            session.execute(boundStatement.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM));
//        }
        //session.execute(boundStatement);
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
        //SELECT * FROM my_keyspace.total_products_by_store WHERE category_name = 'Здоров’я';
        String selectDataQuery = String.format(
                "SELECT * FROM \"%s\".\"%s\" WHERE category_name = '"+cat+"';",
                KEYSPACE_NAME, TABLE_NAME2);

        ResultSet resultSet = session.execute(selectDataQuery);

        //LOGGER.info("Selected data:");
//        for (Row row : resultSet) {
//            LOGGER.info(row.toString());
//        }
        ArrayList<Long> total = new ArrayList<>();
        String Store_address = null;
        String Category_name = null;
        long max = 0;
        for (Row row : resultSet) {
            //HashMap<String, Integer>
            //UUID id = row.getUuid("id");
            Long Total_quantity = row.getLong("total_quantity");
            if (Total_quantity > max) {
                max = Total_quantity;
                Store_address = row.getString("store_address");
                Category_name = row.getString("category_name");
            }
            total.add(Total_quantity);
//            String Category_name = row.getString("category_name");
//            String Store_address = row.getString("store_address");
//            UUID product_id = row.getUuid("product_id");
//            int quantity = row.getInt("quantity");
//            // String description = row.getString("description");
//
//            LOGGER.info("Category_name: {}, Store_address: {}, Product_id: {}, Quantity: {}",
//                    Category_name, Store_address, product_id, quantity);
        }
        LOGGER.info("Category_name: {}, Store_address: {}, Total_Quantity: {}",
                Category_name, Store_address, max);
    }
}
