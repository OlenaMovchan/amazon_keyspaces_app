package com.shpp;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.net.InetSocketAddress;

public class Connector {
    private CqlSession session;
    static String username = "cassandra-at-271420611782";
    static String password = "+YI+MZcwHbAUo1AKOe4gUy1cY4A8dDPXsMrK/V1hQUs=";

    public void connect(String host, int port) {
        Config config = ConfigFactory.load("application.conf");
        DriverConfigLoader configLoader = DriverConfigLoader.fromClasspath("application.conf");
        this.session = CqlSession.builder()
                .withConfigLoader(configLoader)
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter("eu-central-1")
                .withKeyspace("my_keyspace")
                .withAuthCredentials(username, password)
                //.withConfigLoader(DriverConfigLoader.fromClasspath(config.getString("datastax-java-driver.serial-consistency")))
                .build();

    }
    public CqlSession getSession() {
        return this.session;
    }
    public void close() {
        session.close();
    }
    //
    //public void closeSession() {
    //        if (session != null && !session.isClosed()) {
    //            session.close();
    //        }
    //    }
}
