package com.shpp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.shpp.repository.CreationTables;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class CreationTablesTest {

    @Test
    void testCreateTable() {

        CqlSession session = mock(CqlSession.class);

        CreationTables creationTables = new CreationTables();

        creationTables.createTable(session);

        verify(session, times(5)).execute(anyString());

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);

        verify(session, times(5)).execute(queryCaptor.capture());

        assertEquals("CREATE TABLE IF NOT EXISTS my_keyspace.store_product_table_ ("
                        + "category_id UUID, "
                        + "store_id UUID, "
                        + "product_id UUID, "
                        + "quantity INT, "
                        + "PRIMARY KEY ((category_id), store_id, product_id))",
                queryCaptor.getAllValues().get(0));

        assertEquals("CREATE TABLE IF NOT EXISTS my_keyspace.total_products_by_store_ ("
                        + "category_id UUID, "
                        + "store_id UUID, "
                        + "total_quantity COUNTER, "
                        + "PRIMARY KEY (category_id, store_id))",
                queryCaptor.getAllValues().get(1));

        assertEquals("CREATE TABLE IF NOT EXISTS my_keyspace.category_table ("
                        + "category_id UUID, "
                        + "category_name TEXT, "
                        + "PRIMARY KEY ((category_id), category_name))",
                queryCaptor.getAllValues().get(2));

        assertEquals("CREATE TABLE IF NOT EXISTS my_keyspace.store_table ("
                        + "store_id UUID, "
                        + "store_address TEXT, "
                        + "PRIMARY KEY ((store_id), store_address))",
                queryCaptor.getAllValues().get(3));

        assertEquals("CREATE TABLE IF NOT EXISTS my_keyspace.product_table ("
                        + "product_id UUID, "
                        + "product_name TEXT, "
                        + "PRIMARY KEY ((product_id), product_name))",
                queryCaptor.getAllValues().get(4));
    }
}

