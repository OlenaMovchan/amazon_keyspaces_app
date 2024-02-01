package com.shpp.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DataSelection {

    public static final Logger LOGGER = LoggerFactory.getLogger(DataSelection.class);
    private final String KEYSPACE_NAME = "my_keyspace";
    private String categoryTable = "category_table";
    private String storeTable = "store_table";
    private String totalProductsByStore = "total_products_by_store_";
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

            UUID categoryId = findCategoryIdByName(category);

            Pair<UUID, Long> storeData = findLargestQuantityAndStoreId(categoryId);

            if (storeData == null) {
                LOGGER.info("There is no such category in the database: {}", category);
                return;
            }

            String address = findAddressByStoreId(storeData.getLeft());

            LOGGER.info("Category_name: {}, Store_address: {}, Total_Quantity: {}",
                    category, address, storeData.getRight());
        } catch (Exception e) {
            LOGGER.error("Error selecting data", e);
        }
    }

    public UUID findCategoryIdByName(String categoryName) {
        String selectQuery = String.format(
                "SELECT category_id FROM \"%s\".\"%s\" WHERE category_name = ? ALLOW FILTERING;",
                KEYSPACE_NAME, categoryTable);

        ResultSet resultSet = session.execute(selectQuery, categoryName);
        Row row = resultSet.one();
        return (row != null) ? row.getUuid("category_id") : null;
    }

    public Pair<UUID, Long> findLargestQuantityAndStoreId(UUID categoryId) {
        String selectQuery = String.format(
                "SELECT * FROM \"%s\".\"%s\" WHERE category_id = ?;",
                KEYSPACE_NAME, totalProductsByStore);

        ResultSet resultSet = session.execute(selectQuery, categoryId);

        UUID storeId = null;
        long largestAmount = 0;

        for (Row row : resultSet) {
            long totalQuantity = row.getLong("total_quantity");
            if (totalQuantity > largestAmount) {
                largestAmount = totalQuantity;
                storeId = row.getUuid("store_id");
            }
        }
        return (storeId != null) ? Pair.of(storeId, largestAmount) : null;
    }

    public String findAddressByStoreId(UUID storeId) {
        String selectQuery = String.format(
                "SELECT store_address FROM \"%s\".\"%s\" WHERE store_id = ?;",
                KEYSPACE_NAME, storeTable);

        ResultSet resultSet = session.execute(selectQuery, storeId);
        Row row = resultSet.one();
        return (row != null) ? row.getString("store_address") : null;
    }

}
