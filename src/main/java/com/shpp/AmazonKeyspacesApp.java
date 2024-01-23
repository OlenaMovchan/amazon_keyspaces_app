package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.shpp.repository.CreationTables;
import com.shpp.repository.DataSelection;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.shpp.Repository.*;

/**
 * Hello world!
 */
public class AmazonKeyspacesApp {
    static String contactPoint = "cassandra.eu-central-1.amazonaws.com";
    public static int port = 9142;
    public static Logger LOGGER = LoggerFactory.getLogger(AmazonKeyspacesApp.class);
    public static final String KEYSPACE_NAME = "my_keyspace";
    public static final String TABLE_NAME = "store_product_table_";
    private static final String TABLE_NAME2 = "total_products_by_store_";

    public static void main(String[] args) {
        new AmazonKeyspacesApp().run();

    }

    public static void run() {
        String category = System.getProperty("categoryName", "Дім");
        Connector connector = new Connector();
        connector.connect(contactPoint, port);
        CqlSession session = connector.getSession();
        CreationTables creationTables = new CreationTables();
        Repo repository = new Repo();
        DataSelection dataSelection = new DataSelection(session);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        creationTables.createTable(session);

        waitForTableCreation(session, TABLE_NAME);
        waitForTableCreation(session, TABLE_NAME2);
        stopWatch.stop();
        LOGGER.info("Tables created successfully");

        stopWatch.reset();
        stopWatch.start();

        //executeInsert(session);
        repository.insertData(session);
        stopWatch.stop();
        LOGGER.info("Data generation completed");
        LOGGER.info("Generation and insertion time: " + stopWatch.getTime() + " ms"+", speed " + 3000000/(stopWatch.getTime()/1000)+"/s");
        //+", speed " + 3000000/(stopWatch.getTime()/1000)+"/s"

        stopWatch.reset();
        stopWatch.start();

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
