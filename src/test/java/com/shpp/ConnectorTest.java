package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConnectorTest {
    Connector connector = new Connector();
    String contactPoint = "cassandra.eu-central-1.amazonaws.com";
    int port = 9142;

    @Test
    public void testConnect() {
        connector.connect(contactPoint, port);
        CqlSession session = connector.getSession();

        assertNotNull(session);
        assertFalse(session.isClosed());

        connector.close();
        assertTrue(session.isClosed());
    }
}
