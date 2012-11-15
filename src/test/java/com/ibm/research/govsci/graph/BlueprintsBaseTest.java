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
    private static final String VERTEX_TYPE = "dummyType";
    private static final String EDGE_LABEL = "dummyLabel";
    private static final String VERTEX_STRING_PROPERTY = "testStringProperty";
    
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
                {Engine.ORIENTDB, "memory:unittest", null},
                {Engine.NEO4JBATCH, "::folder::", null},
        };
        // data = new Object[][] { {Engine.TITAN, "::folder::", null} };
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
        // neo4jbatch does not support dropping indexes
        if (dbengine.equals(Engine.NEO4JBATCH)) {
            return;
        }
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
        Vertex v = b.createNakedVertex(VERTEX_TYPE);
        b.setElementCreateTime(v);
    }

    @Test
    public void testCreateEdgeIfNotExistObjectVertexVertexString() {
        // neo4jbatch does not support searching edges to see if they
        // already exist
        if (dbengine.equals(Engine.NEO4JBATCH)) {
            return;
        }
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        Vertex v2 = b.createNakedVertex(VERTEX_TYPE);
        Edge e1 = b.createEdgeIfNotExist(null, v1, v2, EDGE_LABEL);
    }

    @Test
    public void testCreateEdgeIfNotExistVertexVertexString() {
        if (dbengine.equals(Engine.NEO4JBATCH)) {
            return;
        }
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        Vertex v2 = b.createNakedVertex(VERTEX_TYPE);
        Edge e1 = b.createEdgeIfNotExist(v1, v2, EDGE_LABEL);
    }

    @Test
    public void testRemoveEdge() {
        // neo4jbatch does not support removing edges
        if (dbengine.equals(Engine.NEO4JBATCH)) {
            return;
        }
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        Vertex v2 = b.createNakedVertex(VERTEX_TYPE);
        Edge e1 = b.createEdgeIfNotExist(v1, v2, EDGE_LABEL);
        b.removeEdge(e1);
    }

    @Test
    public void testCreateNakedVertex() {
        b.createNakedVertex(VERTEX_TYPE);
    }

    @Test
    public void testGetOrCreateVertexHelper() {
        if (dbengine.equals(Engine.NEO4JBATCH)) {
            return;
        }
        Index<Vertex> idx = b.getOrCreateIndex("test-idx");
        Vertex v1 = b.getOrCreateVertexHelper("testIdCol", "testVal", VERTEX_TYPE, idx);
        b.setProperty(v1, VERTEX_STRING_PROPERTY, "foo");
        Vertex v2 = b.getOrCreateVertexHelper("testIdCol", "testVal", VERTEX_TYPE, idx);
        assertTrue(v1.getProperty(VERTEX_STRING_PROPERTY).equals(v2.getProperty(VERTEX_STRING_PROPERTY)));
    }

    @Test
    public void testPropertyToDateObject() {
        Date d = b.propertyToDate(1000000000);
        d = b.propertyToDate(1000000000000L);
        d = b.propertyToDate("2012-02-10T19:22:10+0000");
    }


    @Test
    public void testStopTransaction() {
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        b.stopTransaction();
        Vertex v2 = b.createNakedVertex(VERTEX_TYPE);
    }

    @Test
    public void testRollbackTransaction() {
        if (b.supportsTransactions()) {
            // create the key index
            b.createKeyIndex(VERTEX_STRING_PROPERTY);
            Vertex v1 = b.getOrCreateVertexHelper(VERTEX_STRING_PROPERTY, "test", VERTEX_TYPE, null);
            
            // sub transaction 1
            BlueprintsBase b2 = b.startTransaction();
            v1 = b.getOrCreateVertexHelper(VERTEX_STRING_PROPERTY, "test", VERTEX_TYPE, null);
            b2.setProperty(v1, VERTEX_STRING_PROPERTY, "foo");
            b2.stopTransaction();
            
            // sub transaction 2
            b2 = b.startTransaction();
            v1 = b2.getOrCreateVertexHelper(VERTEX_STRING_PROPERTY, "foo", VERTEX_TYPE, null);
            b2.setProperty(v1, VERTEX_STRING_PROPERTY, "bar");
            assertTrue(v1.getProperty(VERTEX_STRING_PROPERTY).equals("bar"));
            b2.rollbackTransaction();
            
            // check in main transaction loop if anything changed
            v1 = b.getOrCreateVertexHelper(VERTEX_STRING_PROPERTY, "foo", VERTEX_TYPE, null);
            log.warn("XXXXXXX: {}", v1.getProperty(VERTEX_STRING_PROPERTY));
            assertTrue(v1.getProperty(VERTEX_STRING_PROPERTY).equals("foo"));
        }
    }

    @Test
    public void testAddToIndexIfNotPresent() {
        if (dbengine.equals(Engine.NEO4JBATCH)) {
            return;
        }
        if (b.supportsIndexes()) {
            Index<Vertex> idx = b.getOrCreateIndex("test-idx");
            Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
            assertTrue(b.addToIndexIfNotPresent("testIdCol", "testVal", v1, idx));
            assertFalse(b.addToIndexIfNotPresent("testIdCol", "testVal", v1, idx));
        } else {
            log.info("Database engine \"{}\" does not support manual indexes", 
                    b.getDbengine());
        }
    }

    @Test
    public void testSetPropertyElementStringString() {
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        b.setProperty(v1, "testProperty", "testValue");
        assertTrue(v1.getProperty("testProperty").equals("testValue"));
    }

    @Test
    public void testSetPropertyElementStringDate() {
        Date d = new Date();
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        b.setProperty(v1, "testDateProperty", d);
    }

    @Test
    public void testSetPropertyElementStringInt() {
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        b.setProperty(v1, "testIntProperty", 3);
        assertEquals(v1.getProperty("testIntProperty"), 3);
    }

    @Test
    public void testSetPropertyElementStringLong() {
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        b.setProperty(v1, "testLongProperty", 1L);
        assertEquals(v1.getProperty("testLongProperty"), 1L);
    }

    @Test
    public void testSetPropertyElementStringDouble() {
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        b.setProperty(v1, "testDoubleProperty", 1.5);
        assertTrue(Math.abs(((Double)v1.getProperty("testDoubleProperty")) - 1.5) < 0.0001);
    }

    @Test
    public void testSetPropertyElementStringBoolean() {
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        b.setProperty(v1, "testBooleanProperty", true);
        assertEquals(v1.getProperty("testBooleanProperty"), true);
    }

    @Test
    public void testSetPropertyElementStringObject() {
        Object o = new String("foo");
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        b.setProperty(v1, "testObjectProperty", o);
    }

    @Test
    public void testSetPropertyIfNull() {
        Vertex v1 = b.createNakedVertex(VERTEX_TYPE);
        b.setPropertyIfNull(v1, VERTEX_STRING_PROPERTY, "test1");
        b.setPropertyIfNull(v1, VERTEX_STRING_PROPERTY, "test2");
        assertTrue(v1.getProperty(VERTEX_STRING_PROPERTY).equals("test1"));
    }

    @Test
    public void testShutdown() {
        b.shutdown();
    }

}
