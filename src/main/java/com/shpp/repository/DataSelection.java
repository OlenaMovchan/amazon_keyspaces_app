package com.shpp.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DataSelection {

    public static final Logger LOGGER = LoggerFactory.getLogger(DataSelection.class);
    private final String KEYSPACE_NAME = "my_keyspace";
    private final String TABLE_NAME = "store_product_table_";
    private static String categoryTable = "category_table";
    private static String storeTable = "store_table";
    private final String totalProductsByStore = "total_products_by_store_";
    static CqlSession session;

    public DataSelection(CqlSession session) {
        this.session = session;
    }

    public void selectData(String category) {
        try {
            if (session == null) {
                LOGGER.error("CqlSession is null. Cannot execute query.");
                return;
            }
            String select = String.format(
                    "SELECT * FROM \"%s\".\"%s\" ;",//WHERE category_name = '" + category + "'ALLOW FILTERING
                    KEYSPACE_NAME, categoryTable);
            ResultSet resultSet1 = session.execute(select);
           UUID category_id;
           String categoryName = "";
           UUID target = null;
            for (Row row: resultSet1) {
                category_id = row.getUuid("category_id");
                categoryName = row.getString("category_name");
                if (categoryName.equals(category)) {
                    target = category_id;
                    LOGGER.info("Target  " + target);
                }
                //LOGGER.info("UUID   " + category_id);
                //LOGGER.info("Category  " + categoryName);
            }
            if (target.equals(null)) {
                LOGGER.info("There is no such category in the database: {}", category);
            }//32e6c81d-fc29-4c29-bf40-a20751f1bb1f
            String selectDataQuery = String.format(
                    "SELECT * FROM \"%s\".\"%s\" WHERE category_id = ?;",
                    KEYSPACE_NAME, totalProductsByStore);

            ResultSet resultSet = session.execute(selectDataQuery, target);

            UUID storeId = null;

            long largestAmount = 0;
            for (Row row : resultSet) {
                Long totalQuantity = row.getLong("total_quantity");
                if (totalQuantity > largestAmount) {
                    largestAmount = totalQuantity;
                    storeId = row.getUuid("store_id");

                }
            }
            String select3 = String.format(
                    "SELECT * FROM \"%s\".\"%s\" WHERE store_id = ?;",
                    KEYSPACE_NAME, storeTable);

            ResultSet resultSet2 = session.execute(select3, storeId);
            String address = "";
            for (Row row: resultSet2) {
                LOGGER.info("Address   " +row.getString("store_address"));
                if (storeId.equals(row.getUuid("store_id"))){
                    address = row.getString("store_address");
                }
            }
            LOGGER.info("Category_name: {}, Store_address: {}, Total_Quantity: {}",
                    category, address, largestAmount);
        } catch (Exception e) {
            LOGGER.error("Error selecting data: {}", e.getMessage());
        }
    }
}
