package com.shpp.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreationTables {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreationTables.class);
    private String KEYSPACE_NAME = "my_keyspace";
    private String storeProductTable = "store_product_table_";
    private String totalProductsByStore = "total_products_by_store_";
    private String categoryTable = "category_table";
    private String storeTable = "store_table";
    private String productTable = "product_table";

    public void createTable(CqlSession session) {
        try {
            if (session == null) {
                LOGGER.error("CqlSession is null. Cannot execute CREATE TABLE queries.");
                return;
            }
            String createTableQuery = String.format(
                    "CREATE TABLE IF NOT EXISTS %s.%s ("
                            //+ "category_id UUID, "
                            + "store_id UUID, "
                            + "product_id UUID, "
                            + "quantity INT, "
                            + "PRIMARY KEY (store_id))",
                    KEYSPACE_NAME, storeProductTable);
            session.execute(createTableQuery);

            String createTableQuery2 = String.format(
                    "CREATE TABLE IF NOT EXISTS %s.%s ("
                            + "category_id UUID, "
                            + "store_id UUID, "
                            + "total_quantity COUNTER, "
                            + "PRIMARY KEY (category_id))",
                    KEYSPACE_NAME, totalProductsByStore);
            session.execute(createTableQuery2);

            String createTableQuery3 = String.format(
                    "CREATE TABLE IF NOT EXISTS %s.%s ("
                            + "category_id UUID, "
                            + "category_name TEXT, "
                            + "PRIMARY KEY (category_id))",
                    KEYSPACE_NAME, categoryTable);
            session.execute(createTableQuery3);

            String createTableQuery4 = String.format(
                    "CREATE TABLE IF NOT EXISTS %s.%s ("
                            + "store_id UUID, "
                            + "store_address TEXT, "
                            + "PRIMARY KEY (store_id))",
                    KEYSPACE_NAME, storeTable);
            session.execute(createTableQuery4);

            String createTableQuery5 = String.format(
                    "CREATE TABLE IF NOT EXISTS %s.%s ("
                            + "product_id UUID, "
                            + "product_name TEXT, "
                            + "category_id UUID, "
                            + "PRIMARY KEY (product_id))",
                    KEYSPACE_NAME, productTable);
            session.execute(createTableQuery5);

            LOGGER.info("Table created successfully");
        } catch (Exception e) {
            LOGGER.error("Error creating tables: {}", e.getMessage());
        }
    }
}
