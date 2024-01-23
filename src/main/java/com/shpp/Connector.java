package com.shpp;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class Connector {
    private CqlSession session;
    static String username = "cassandra-at-271420611782";
    static String password = "+YI+MZcwHbAUo1AKOe4gUy1cY4A8dDPXsMrK/V1hQUs=";
    public static final Logger LOGGER = LoggerFactory.getLogger(Connector.class);
    public void connect(String host, int port) {
        Config config = ConfigFactory.load("application.conf");
        DriverConfigLoader configLoader = DriverConfigLoader.fromClasspath("application.conf");
        this.session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter("eu-central-1")
                .withKeyspace("my_keyspace")
                .withAuthCredentials(username, password)
                //.withConfigLoader(configLoader)
                .build();

        String consistencyLevel = session.getContext().getConfig().getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY);
        LOGGER.info("Consistency level: " + consistencyLevel);
    }

    public CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }

}
