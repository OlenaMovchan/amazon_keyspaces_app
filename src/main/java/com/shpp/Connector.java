package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;

public class Connector {
    private CqlSession session;
    private String username = "cassandra_amazon-at-211125399931";
    private String password = "4BzSxWYLvUhC8LBnrQF4x1y/aFCiNYjTigN3Y56hP9c=";
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
            LOGGER.error("Error connecting to AmazonKeyspaces: {}", e.getMessage());
        }
    }

    public CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }

}
