package com.shpp.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.shpp.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSelection {

    private static final Logger LOGGER = LoggerFactory.getLogger(Repository.class);
    private static final String KEYSPACE_NAME = "my_keyspace";
    private static final String TABLE_NAME = "store_product_table_";
    private static final String TABLE_NAME2 = "total_products_by_store_";
    static CqlSession session;

    public DataSelection(CqlSession session) {
        this.session = session;
    }

    public void selectData(String category) {
        String selectDataQuery = String.format(
                "SELECT * FROM \"%s\".\"%s\" WHERE category_name = '" + category + "';",
                KEYSPACE_NAME, TABLE_NAME2);

        ResultSet resultSet = session.execute(selectDataQuery);

        String storeAddress = null;
        String categoryName = null;
        long largestAmount = 0;
        for (Row row : resultSet) {
            Long totalQuantity = row.getLong("total_quantity");
            if (totalQuantity > largestAmount) {
                largestAmount = totalQuantity;
                storeAddress = row.getString("store_address");
                categoryName = row.getString("category_name");
            }
        }
        LOGGER.info("Category_name: {}, Store_address: {}, Total_Quantity: {}",
                categoryName, storeAddress, largestAmount);
    }
}
