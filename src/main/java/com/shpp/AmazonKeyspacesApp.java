package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import static com.shpp.Repository.*;

/**
 * Hello world!
 *
 */
public class AmazonKeyspacesApp {
    static String contactPoint = "cassandra.eu-central-1.amazonaws.com";
    static String username = "cassandra-at-271420611782";
    static String password = "+YI+MZcwHbAUo1AKOe4gUy1cY4A8dDPXsMrK/V1hQUs=";
    private static int port = 9142;
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonKeyspacesApp.class);
    private static final String KEYSPACE_NAME = "my_keyspace";
    private static final String TABLE_NAME = "store_product_table";

    public static void main(String[] args) {
        new AmazonKeyspacesApp().run();

    }
    public static void run(){
        System.out.println("Start programm");
        System.out.println("20-01-19-56");
        //try (CqlSession session = CqlSession.builder()
        Connector connector = new Connector();
        connector.connect(contactPoint, port);
        CqlSession session = connector.getSession();
//        CqlSession session = CqlSession.builder()
//                .addContactPoint(new InetSocketAddress(contactPoint, port))
//                .withLocalDatacenter("eu-central-1")
//                .withKeyspace(KEYSPACE_NAME)
//                .withAuthCredentials(username, password)
//                .build();//) //{
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        createTable(session);

        waitForTableCreation(session, TABLE_NAME);
        stopWatch.stop();
        LOGGER.info("Tables created successfully");


        stopWatch.reset();

        stopWatch.start();
        insertData(session);
        stopWatch.stop();
        LOGGER.info("Data generation completed");
        LOGGER.info("Generation and insertion time: " + stopWatch.getTime() + " ms");

        stopWatch.reset();
        stopWatch.start();
        //selectData(session);
        stopWatch.stop();
        LOGGER.info("Query time: " + stopWatch.getTime() + " ms");

        connector.close();
        LOGGER.info("Connector close");
        //session.close();
//        } catch (Exception e) {
//            LOGGER.error("Error ", e.getMessage());
//        }
    }
    private static void waitForTableCreation(CqlSession session, String tableName) {
        long startTime = System.currentTimeMillis();
        long timeoutMillis = TimeUnit.MINUTES.toMillis(5); // Set your desired timeout

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            if (isTableCreated(session, tableName)) {
                LOGGER.info("Table '{}' has been created", tableName);
                return;
            }

            try {
                TimeUnit.SECONDS.sleep(5); // Check every 5 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for table creation", e);
            }
        }

        throw new RuntimeException("Timeout waiting for table creation");
    }

    private static boolean isTableCreated(CqlSession session, String tableName) {
        String query = String.format(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s';",
                KEYSPACE_NAME, tableName);

        return !session.execute(query).all().isEmpty();
    }

}
