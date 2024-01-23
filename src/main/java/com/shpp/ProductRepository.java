package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;

public class ProductRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProductRepository.class);
    private static final String KEYSPACE_NAME = "my_keyspace";
    private static final String TABLE_NAME = "store_product_table";
    private static final String TABLE_NAME2 = "total_products_by_store";

    private final CqlSession session;

    public ProductRepository(CqlSession session) {
        this.session = session;
    }

    public void createTable() {
        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s ("
                        + "category_name TEXT, "
                        + "store_address TEXT, "
                        + "product_id UUID, "
                        + "quantity INT, "
                        + "PRIMARY KEY ((category_name), store_address, product_id))",
                KEYSPACE_NAME, TABLE_NAME);
        session.execute(createTableQuery);
    }

    public void insertProductsInBatches(int totalProducts, int totalStores, int totalCategories, int batchSize) {
        Random random = new Random();
        String[] storeAddresses = new String[totalStores];
        String[] categories = new String[totalCategories];

        for (int i = 0; i < totalStores; i++) {
            storeAddresses[i] = "Store_" + i;
        }

        for (int i = 0; i < totalCategories; i++) {
            categories[i] = "Category_" + i;
        }

        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);


            for (int k = 0; k < totalStores; k++) {
                for (int l = 0; l < totalProducts; l++) {
                    UUID productId = UUID.randomUUID();
                    int quantity = random.nextInt(999);

                    batchBuilder.addStatement(createInsertStatement(categories[quantity], storeAddresses[k], productId, quantity));

                    if (batchBuilder.getStatementsCount() >= batchSize) {
                        executeBatch(batchBuilder.build());
                        batchBuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
                    }
                }
            }



        if (batchBuilder.getStatementsCount() < batchSize) {
            executeBatch(batchBuilder.build());
        }

        LOGGER.info("Data inserted successfully");
    }

    public BoundStatement createInsertStatement(String categoryName, String storeAddress, UUID productId, int quantity) {
        String insertDataQuery = String.format(
                "INSERT INTO %s.%s (category_name, store_address, product_id, quantity) VALUES (?, ?, ?, ?)",
                KEYSPACE_NAME, TABLE_NAME);

        return session.prepare(insertDataQuery).bind()
                .setString("category_name", categoryName)
                .setString("store_address", storeAddress)
                .setUuid("product_id", productId)
                .setInt("quantity", quantity);
    }

    public void executeBatch(BatchStatement batch) {
        session.execute(batch.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE));
    }
}
