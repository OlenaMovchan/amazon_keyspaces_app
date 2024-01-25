package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;

public class Connector {
    private CqlSession session;
    private String username = "cassandra-at-271420611782";
    private String password = "+YI+MZcwHbAUo1AKOe4gUy1cY4A8dDPXsMrK/V1hQUs=";
    public static final Logger LOGGER = LoggerFactory.getLogger(Connector.class);

    public void connect(String host, int port) {

        try {
            this.session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(host, port))
                    .withLocalDatacenter("eu-central-1")
                    .withKeyspace("my_keyspace")
                    .withAuthCredentials(username, password)
                    .build();

            String consistencyLevel = session.getContext().getConfig().getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY);
            LOGGER.info("Consistency level: " + consistencyLevel);

        } catch (Exception e) {
            LOGGER.error("Error connecting to Cassandra: {}", e.getMessage());
        }
    }

    public CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }

}
