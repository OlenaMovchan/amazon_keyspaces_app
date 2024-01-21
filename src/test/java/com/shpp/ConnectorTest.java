package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConnectorTest {
    Connector connector = new Connector();
    static String contactPoint = "cassandra.eu-central-1.amazonaws.com";
    static String username = "cassandra-at-271420611782";
    static String password = "+YI+MZcwHbAUo1AKOe4gUy1cY4A8dDPXsMrK/V1hQUs=";
    private static int port = 9142;

    @Test
    public void testConnect() throws Exception {
        connector.connect(contactPoint, port);
        CqlSession session = connector.getSession();

        assertNotNull(session);
        assertFalse(session.isClosed());

        connector.close();
        assertTrue(session.isClosed());
    }

}
