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

        verify(session, times(2)).execute(anyString());

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);

        verify(session, times(2)).execute(queryCaptor.capture());

        assertEquals("CREATE TABLE IF NOT EXISTS my_keyspace.store_product_table_ (category_name TEXT, store_address TEXT, product_id UUID, quantity INT, PRIMARY KEY ((category_name), store_address, product_id))",
                queryCaptor.getAllValues().get(0));
        assertEquals("CREATE TABLE IF NOT EXISTS my_keyspace.total_products_by_store_ (category_name TEXT, store_address TEXT, total_quantity COUNTER, PRIMARY KEY (category_name, store_address))",
                queryCaptor.getAllValues().get(1));
    }
}

