package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.shpp.dto.CategoryDto;
import com.shpp.dto.ProductDto;
import com.shpp.dto.StoreDto;
import com.shpp.repository.*;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class AmazonKeyspacesApp {
    static String contactPoint = "cassandra.eu-central-1.amazonaws.com";
    public static int port = 9142;
    public static Logger LOGGER = LoggerFactory.getLogger(AmazonKeyspacesApp.class);
    public static final String KEYSPACE_NAME = "my_keyspace";
    public static final String STORE_PRODUCT_TABLE = "store_product_table_";
    private static final String TOTAL_PRODUCTS_BY_STORE = "total_products_by_store_";
    private static String categoryTable = "category_table";
    private static String storeTable = "store_table";
    private static String productTable = "product_table";
// 3 норм форма, модель даних багато повторів, тип текст --по ключикам, COUNTER ---INT
    //UUID для noSql немає транзакцій

    //thread - 1:
    //Amount of data: 750_000
    //Generation and insertion time: 4186102 ms
    //insertion speed: 179 inserts/s
    //Query time: 8 ms

    //thread - 10:
    //Amount of data: 100_000
    //Generation and insertion time: 27608 ms
    //insertion speed: 3622 inserts/s

    public static void main(String[] args) {
        new AmazonKeyspacesApp().run();
    }

    public static void run() {
        //DataInsertion repository = new DataInsertion();

        Connector connector = new Connector();
        connector.connect(contactPoint, port);
        CqlSession session = connector.getSession();

        CreationTables creationTables = new CreationTables();
        DataIns ins = new DataIns(session,KEYSPACE_NAME, STORE_PRODUCT_TABLE, TOTAL_PRODUCTS_BY_STORE, productTable, storeTable, categoryTable);
        DataSelection dataSelection = new DataSelection(session);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        creationTables.createTable(session);

        waitForTableCreation(session, STORE_PRODUCT_TABLE);
        waitForTableCreation(session, TOTAL_PRODUCTS_BY_STORE);
        waitForTableCreation(session, storeTable);
        waitForTableCreation(session, productTable);
        waitForTableCreation(session, categoryTable);
        stopWatch.stop();
        LOGGER.info("Tables created successfully");

        stopWatch.reset();
        stopWatch.start();
        DataGenerator dataGenerator = new DataGenerator();
        int totalProducts = 20;
        int totalCategories = 10;
        int totalStores = 5;
        List<CategoryDto> categoryData = dataGenerator.generateCategoryData(totalCategories);
        List<StoreDto> storeData = dataGenerator.generateStoreData(totalStores);
        List<ProductDto> productData = dataGenerator.generateProductData(totalProducts, categoryData);
        try {
            ins.insertStoreDataWithTry(session, storeData);
            ins.insertCategoryDataWithTry(session, categoryData);
            ins.insertProductDataWithTry(session,productData);

            ins.insertStoreProductDataParallelWithTry(storeData, productData, categoryData);
        } catch (Exception e) {
            LOGGER.error("Error insert" , e);
            System.exit(1);
        }


        stopWatch.stop();
        LOGGER.info("Data generation completed");
        LOGGER.info("Generation and insertion time: " + stopWatch.getTime() + " ms");

        stopWatch.reset();
        stopWatch.start();

        String category = System.getProperty("categoryName", "Дім");
        dataSelection.selectData(category);

        stopWatch.stop();
        LOGGER.info("Query time: " + stopWatch.getTime() + " ms");

        connector.close();
        LOGGER.info("Connector close");
    }

    public static void waitForTableCreation(CqlSession session, String tableName) {
        long startTime = System.currentTimeMillis();
        long timeoutMillis = TimeUnit.MINUTES.toMillis(5);

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            if (isTableCreated(session, tableName)) {
                LOGGER.info("Table '{}' has been created", tableName);
                return;
            }
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for table creation", e);
            }
        }
        throw new RuntimeException("Timeout waiting for table creation");
    }

    public static boolean isTableCreated(CqlSession session, String tableName) {
        String query = String.format(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s';",
                KEYSPACE_NAME, tableName);

        return !session.execute(query).all().isEmpty();
    }
}
