package com.shpp.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.shpp.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreationTables {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreationTables.class);
    private String KEYSPACE_NAME = "my_keyspace";
    private String TABLE_NAME = "store_product_table_";
    private String TABLE_NAME2 = "total_products_by_store_";

    public void createTable(CqlSession session) {
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
}
