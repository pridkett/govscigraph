package com.ibm.research.govsci.graph;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
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
        Object[][] data = new Object[][] { {"tinkergraph", null, null},
                {"tinkergraph", "::nofolder::", null},
                {"neo4j", "::folder::", null},
                {"titan", "::folder::", null},
                {"orientdb", "::folder::", null}
        };
        return Arrays.asList(data);
    }
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    @Before
    public void createBlueprintsBase() {
        try {
            String databaseUrl = dburl;
            log.trace("Creating database: {} {}", dbengine, dburl);
            if (dburl != null && dburl.equals("::folder::")) {
                databaseUrl = folder.newFolder().getAbsolutePath();
            } else if (dburl != null && dburl.equals("::nofolder::")) {
                databaseUrl = new File(folder.newFolder().getAbsolutePath(), "tmp").getAbsolutePath();
            }
            log.trace("attempting to create...");
            b = new BlueprintsBase(dbengine, databaseUrl);
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
        fail("Not yet implemented");
    }

    @Test
    public void testBlueprintsBaseStringString() {
        try {
            String databaseUrl = dburl;
            log.trace("Creating database: {} {}", dbengine, dburl);
            if (dburl != null && dburl.equals("::folder::")) {
                databaseUrl = folder.newFolder().getAbsolutePath();
            } else if (dburl != null && dburl.equals("::nofolder::")) {
                databaseUrl = new File(folder.newFolder().getAbsolutePath(), "tmp").getAbsolutePath();
            }
            log.trace("attempting to create...");
            BlueprintsBase b = new BlueprintsBase(dbengine, databaseUrl);
            log.trace("Success!");
        } catch (IOException e) {
            log.error("IOException creating database:", e);
        }
    }

    @Test
    public void testDropKeyIndex() {
        fail("Not yet implemented");
    }

    @Test
    public void testDropIndex() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetOrCreateIndexStringClassOfT() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetOrCreateIndexString() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetOrCreateEdgeIndex() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetElementCreateTime() {
        fail("Not yet implemented");
    }

    @Test
    public void testCreateEdgeIfNotExistObjectVertexVertexString() {
        fail("Not yet implemented");
    }

    @Test
    public void testCreateEdgeIfNotExistVertexVertexString() {
        fail("Not yet implemented");
    }

    @Test
    public void testRemoveEdge() {
        fail("Not yet implemented");
    }

    @Test
    public void testCreateNakedVertex() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetOrCreateVertexHelper() {
        fail("Not yet implemented");
    }

    @Test
    public void testPropertyToDateInt() {
        fail("Not yet implemented");
    }

    @Test
    public void testPropertyToDateLong() {
        fail("Not yet implemented");
    }

    @Test
    public void testPropertyToDateString() {
        fail("Not yet implemented");
    }

    @Test
    public void testPropertyToDateObject() {
        fail("Not yet implemented");
    }

    @Test
    public void testDateDifference() {
        fail("Not yet implemented");
    }

    @Test
    public void testStopTransaction() {
        fail("Not yet implemented");
    }

    @Test
    public void testRollbackTransaction() {
        fail("Not yet implemented");
    }

    @Test
    public void testAddToIndexIfNotPresent() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetPropertyElementStringString() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetPropertyElementStringDate() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetPropertyElementStringInt() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetPropertyElementStringLong() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetPropertyElementStringDouble() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetPropertyElementStringBoolean() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetPropertyElementStringObject() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetPropertyIfNull() {
        fail("Not yet implemented");
    }

    @Test
    public void testShutdown() {
        fail("Not yet implemented");
    }

}
