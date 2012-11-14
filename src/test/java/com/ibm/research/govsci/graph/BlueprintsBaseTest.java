package com.ibm.research.govsci.graph;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;

import scala.actors.threadpool.Arrays;

@RunWith(value=Parameterized.class)
public class BlueprintsBaseTest {

    private static final Logger log = LoggerFactory.getLogger(BlueprintsBaseTest.class);
    private String dbengine = null;
    private String dburl = null;
    private Map<String, String> config = null;
    private BlueprintsBase b;
    
    public BlueprintsBaseTest(String dbengine, String dburl, Map<String, String> config) {
        this.dbengine = dbengine;
        this.dburl = dburl;
        this.config = config;
    }
    
    @Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] { {Engine.TINKERGRAPH, null, null},
                {Engine.TINKERGRAPH, "::nofolder::", null},
                {Engine.NEO4J, "::folder::", null},
                {Engine.TITAN, "::folder::", null},
                {Engine.ORIENTDB, "memory:unittest", null}
        };
        return Arrays.asList(data);
    }
    
    private String rewriteUrl(String dburl) throws IOException{
        String databaseUrl = dburl;
        if (dburl != null && dburl.equals("::folder::")) {
            databaseUrl = folder.newFolder().getAbsolutePath();
        } else if (dburl != null && dburl.equals("::nofolder::")) {
            databaseUrl = new File(folder.newFolder().getAbsolutePath(), "tmp").getAbsolutePath();
        }
        return databaseUrl;
    }
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    @Before
    public void createBlueprintsBase() {
        log.trace("Creating database: {} {}", dbengine, dburl);
        try {
            log.trace("attempting to create...");
            b = new BlueprintsBase(dbengine, rewriteUrl(dburl));
            log.trace("Success!");
        } catch (IOException e) {
            log.error("IOException creating database:", e);
        }
    }
    
    @After
    public void shutdownBlueprintsBase() {
        b.shutdown();
    }

    @Test
    public void testBlueprintsBaseStringStringMapOfStringString() {
        log.trace("Creating database: {} {}", dbengine, dburl);
        try {
            log.trace("attempting to create...");
            b = new BlueprintsBase(dbengine, rewriteUrl(dburl), config);
            log.trace("Success!");
        } catch (IOException e) {
            log.error("IOException creating database:", e);
        }
    }

    @Test
    public void testBlueprintsBaseStringString() {
        log.trace("Creating database: {} {}", dbengine, dburl);
        try {
            log.trace("attempting to create...");
            BlueprintsBase b = new BlueprintsBase(dbengine, rewriteUrl(dburl));
            log.trace("Success!");
        } catch (IOException e) {
            log.error("IOException creating database:", e);
        }
    }

    @Test
    public void testDropKeyIndex() {
        b.createKeyIndex("testIndex");
        b.dropKeyIndex("testIndex");
    }

    @Test
    public void testDropIndex() {
        b.getOrCreateIndex("test-idx");
        b.dropIndex("test-idx");
    }

    @Test
    public void testGetOrCreateIndexStringClassOfT() {
        b.getOrCreateIndex("test-vertex-idx", Vertex.class);
        b.getOrCreateIndex("test-edge-idx", Edge.class);
    }

    @Test
    public void testGetOrCreateIndexString() {
        b.getOrCreateIndex("test-vertex-idx");
    }

    @Test
    public void testGetOrCreateEdgeIndex() {
        b.getOrCreateEdgeIndex("test-edge-idx");
    }

    @Test
    public void testSetElementCreateTime() {
        Vertex v = b.createNakedVertex("dummyType");
        b.setElementCreateTime(v);
    }

    @Test
    public void testCreateEdgeIfNotExistObjectVertexVertexString() {
        Vertex v1 = b.createNakedVertex("dummyType");
        Vertex v2 = b.createNakedVertex("dummyType");
        Edge e1 = b.createEdgeIfNotExist(null, v1, v2, "dummyLabel");
    }

    @Test
    public void testCreateEdgeIfNotExistVertexVertexString() {
        Vertex v1 = b.createNakedVertex("dummyType");
        Vertex v2 = b.createNakedVertex("dummyType");
        Edge e1 = b.createEdgeIfNotExist(v1, v2, "dummyLabel");
    }

    @Test
    public void testRemoveEdge() {
        Vertex v1 = b.createNakedVertex("dummyType");
        Vertex v2 = b.createNakedVertex("dummyType");
        Edge e1 = b.createEdgeIfNotExist(v1, v2, "dummyLabel");
        b.removeEdge(e1);
    }

    @Test
    public void testCreateNakedVertex() {
        b.createNakedVertex("dummyType");
    }

    @Test
    public void testGetOrCreateVertexHelper() {
        Index<Vertex> idx = b.getOrCreateIndex("test-idx");
        Vertex v1 = b.getOrCreateVertexHelper("testIdCol", "testVal", "dummyType", idx);
        b.setProperty(v1, "testStringProperty", "foo");
        Vertex v2 = b.getOrCreateVertexHelper("testIdCol", "testVal", "dummyType", idx);
        assertTrue(v1.getProperty("testStringProperty").equals(v2.getProperty("testStringProperty")));
    }

    @Test
    public void testPropertyToDateObject() {
        Date d = b.propertyToDate(1000000000);
        d = b.propertyToDate(1000000000000L);
        d = b.propertyToDate("2012-02-10T19:22:10+0000");
    }


    @Test
    public void testStopTransaction() {
        Vertex v1 = b.createNakedVertex("dummyType");
        b.stopTransaction();
        Vertex v2 = b.createNakedVertex("dummyType");
    }

    @Test
    public void testRollbackTransaction() {
        if (b.supportsTransactions()) {
            Vertex v1 = b.createNakedVertex("dummyType");
            b.setProperty(v1, "testStringProperty", "foo");
            b.stopTransaction();
            b.setProperty(v1, "testStringProperty", "bar");
            b.rollbackTransaction();
            assertTrue(v1.getProperty("testStringProperty").equals("foo"));
        }
    }

    @Test
    public void testAddToIndexIfNotPresent() {
        if (b.supportsIndexes()) {
            Index<Vertex> idx = b.getOrCreateIndex("test-idx");
            Vertex v1 = b.createNakedVertex("dummyType");
            assertTrue(b.addToIndexIfNotPresent("testIdCol", "testVal", v1, idx));
            assertFalse(b.addToIndexIfNotPresent("testIdCol", "testVal", v1, idx));
        } else {
            log.info("Database engine \"{}\" does not support manual indexes", 
                    b.getDbengine());
        }
    }

    @Test
    public void testSetPropertyElementStringString() {
        Vertex v1 = b.createNakedVertex("dummyType");
        b.setProperty(v1, "testProperty", "testValue");
        assertTrue(v1.getProperty("testProperty").equals("testValue"));
    }

    @Test
    public void testSetPropertyElementStringDate() {
        Date d = new Date();
        Vertex v1 = b.createNakedVertex("dummyType");
        b.setProperty(v1, "testDateProperty", d);
    }

    @Test
    public void testSetPropertyElementStringInt() {
        Vertex v1 = b.createNakedVertex("dummyType");
        b.setProperty(v1, "testIntProperty", 3);
        assertEquals(v1.getProperty("testIntProperty"), 3);
    }

    @Test
    public void testSetPropertyElementStringLong() {
        Vertex v1 = b.createNakedVertex("dummyType");
        b.setProperty(v1, "testLongProperty", 1L);
        assertEquals(v1.getProperty("testLongProperty"), 1L);
    }

    @Test
    public void testSetPropertyElementStringDouble() {
        Vertex v1 = b.createNakedVertex("dummyType");
        b.setProperty(v1, "testDoubleProperty", 1.5);
        assertTrue(Math.abs(((Double)v1.getProperty("testDoubleProperty")) - 1.5) < 0.0001);
    }

    @Test
    public void testSetPropertyElementStringBoolean() {
        Vertex v1 = b.createNakedVertex("dummyType");
        b.setProperty(v1, "testBooleanProperty", true);
        assertEquals(v1.getProperty("testBooleanProperty"), true);
    }

    @Test
    public void testSetPropertyElementStringObject() {
        Object o = new String("foo");
        Vertex v1 = b.createNakedVertex("dummyType");
        b.setProperty(v1, "testObjectProperty", o);
    }

    @Test
    public void testSetPropertyIfNull() {
        Vertex v1 = b.createNakedVertex("dummyType");
        b.setPropertyIfNull(v1, "testStringProperty", "test1");
        b.setPropertyIfNull(v1, "testStringProperty", "test2");
        assertTrue(v1.getProperty("testStringProperty").equals("test1"));
    }

    @Test
    public void testShutdown() {
        b.shutdown();
    }

}
