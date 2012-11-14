/**
 * BlueprintsBase.java
 *
 * A variety of abstractions useful for graph databases.
 * 
 * Copyright (c) 2011-2012 IBM Corporation
 *
 * This library was originally developed for a joint research
 * project with the University of Nebraska, Lincoln under terms
 * of the Joint Study Agreement between IBM and UNL.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Patrick Wagstrom <patrick@wagstrom.net>
 */

package com.ibm.research.govsci.graph;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.impls.neo4jbatch.Neo4jBatchGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.rexster.RexsterGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

/**
 * @author patrick
 *
 */
public class BlueprintsBase implements Shutdownable {
    private Logger log = null;
    protected IndexableGraph igraph = null;
    protected KeyIndexableGraph kigraph = null;
    protected TransactionalGraph tgraph = null;
    protected SimpleDateFormat dateFormatter = null;
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    private static final String INDEX_TYPE = "type-idx";
    private static final String PROPERTY_TYPE = "type";

    protected Index<Vertex> typeidx = null;

    /**
     * Full constructor that takes an engine, a url, and a map for a configuration
     * 
     * Configuration parameters should be the exact names that the database uses. This
     * is mainly for setting very specific parameters for neo4j, but it also works for
     * defining a username and password for connecting to an OreintDB database.
     * 
     * @param engine name of the engine
     * @param dburl url of the database for the engine
     * @param config parameters for the engine
     */
    public BlueprintsBase(String engine, String dburl, Map<String, String> config) {
        log = LoggerFactory.getLogger(BlueprintsBase.class);
        String eng = engine.toLowerCase().trim();
        log.warn("Requested database: {} url: {}", eng, dburl);
        if (eng.equals("neo4j")) {
            log.info("Opening neo4j graph at: {}", dburl);
            kigraph = new Neo4jGraph(dburl, config);
            tgraph = (TransactionalGraph) kigraph;
            igraph = (IndexableGraph) kigraph;
        } else if (eng.equals("rexster")) {
            log.warn("Configuration parameters passed to RexsterGraph - Ignored");
            log.info("Opening rexster graph at: {}", dburl);
            kigraph = new RexsterGraph(dburl);
            igraph = (IndexableGraph) kigraph;
        } else if (eng.equals("tinkergraph")) {
            if (config != null) {
                log.warn("Configuration parameters passed to TinkerGraph - Ignored");
            }
            log.info("creating new tinkergraph with url: {}", dburl);
            if (dburl == null) {
                kigraph = new TinkerGraph();
            } else {
                kigraph = new TinkerGraph(dburl);
            }
            igraph = (IndexableGraph) kigraph;
        } else if (eng.equals("neo4jbatch")) {
            log.info("Opening neo4j batch graph at: {}", dburl);
            kigraph = new Neo4jBatchGraph(dburl, config);
            tgraph = (TransactionalGraph) kigraph;
            igraph = (IndexableGraph) kigraph;
        } else if (eng.equals("orientdb")) {
            String username = config.get("username");
            String password = config.get("password");
            if (username != null && password != null) {
                kigraph = new OrientGraph(dburl, username, password);
            } else {
                kigraph = new OrientGraph(dburl);
            }
            tgraph = (TransactionalGraph) kigraph;
            igraph = (IndexableGraph) kigraph;
        } else if (eng.equals("titan")) {
            Configuration conf = null;
            TitanGraph g;
            if (config != null) {
                conf = new BaseConfiguration();
                for (Entry<String, String> e : config.entrySet()) {
                    conf.setProperty(e.getKey(), e.getValue());
                }
            }
            if (conf != null) {
                g = TitanFactory.open(conf);
            } else {
                g = TitanFactory.open(dburl);
            }
            kigraph = (KeyIndexableGraph) g;
            // igraph = (IndexableGraph) g;
            tgraph = (TransactionalGraph) g;
        } else {
            log.error("Undefined database engine: {}", eng);
            System.exit(-1);
        }

        if (igraph != null) {
            log.debug("attempting to fetch index: {}", INDEX_TYPE);
            typeidx = getOrCreateIndex(INDEX_TYPE);
        }
        if (kigraph != null) {
            createKeyIndex(PROPERTY_TYPE);
        }
        
        dateFormatter = new SimpleDateFormat(DATE_FORMAT);
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /**
     * Simple constructor that takes an engine and a url for the database
     * 
     * @param engine name of the engine
     * @param dburl url of the database for the engine
     */
    public BlueprintsBase(String engine, String dburl) {
        this(engine, dburl, null);
    }

    public void dropKeyIndex(String key) {
        dropKeyIndex(key, Vertex.class);
    }

    /**
     * Drops an index from the specific graph
     * 
     * @param key the name of the index to drop
     * @param elementClass the class of element to index
     */
    public <T extends Element> void dropKeyIndex(String key, Class <T> elementClass) {
        if (kigraph == null) {
            log.error("dropKeyIndex - graph is not KeyIndexableGraph");
        } else {
            kigraph.dropKeyIndex(key, elementClass);
        }
    }
    
    /**
     * Drops a manual index from the graph
     * 
     * @deprecated
     * @param idxname
     */
    public void dropIndex(String idxname) {
        if (igraph == null) {
            log.error("dropIndex - graph is not IndexableGraph");
        } else {
            igraph.dropIndex(idxname);
        }
    }

    /**
     * Gets a reference to the specified index, creating it if it doesn't exist.
     * 
     * This probably could be better written if it used generics or something like that
     * 
     * @param idxname the name of the index to load/create
     * @param indexClass the class the index should use, either Vertex or Edge
     * @return a reference to the loaded/created index
     */
    public <T extends Element> Index<T> getOrCreateIndex(String idxname, Class<T> idxClass) {
        Index<T> idx = null;
        if (igraph == null) {
            log.error("getOrCreateIndex - graph is not IndexableGraph");
        }
        log.trace("Getting index: {} type: {}", idxname, idxClass.toString());
        try {
            idx = igraph.getIndex(idxname, idxClass);
        } catch (NullPointerException e) {
            log.error("Null pointer exception fetching index: {} {}", idxname, e);
        } catch (RuntimeException e) {
            log.debug("Runtime exception encountered getting index {}. Upgrade to newer version of blueprints.", idxname);
        }
        if (idx == null) {
            log.warn("Creating index {} for class {}", idxname, idxClass.toString());
            idx = igraph.createIndex(idxname, idxClass);
        }
        return idx;
    }

    /**
     * Helper function to get Vertex indexes
     * 
     * @param idxname name of the index to retrieve
     * @return the index if it exists, or a new index if it does not
     */
    public Index<Vertex> getOrCreateIndex(String idxname) {
        log.trace("Getting vertex index: {}", idxname);
        return getOrCreateIndex(idxname, Vertex.class);
    }

    
    /**
     * Helper function to get Edge indexes
     * 
     * @param idxname the name of the index to retrieve
     * @return the index if it exists, or a new index if it does not
     */
    public Index<Edge> getOrCreateEdgeIndex(String idxname) {
        return (Index<Edge>)getOrCreateIndex(idxname, Edge.class);
    }

    /**
     * used for KeyIndexableGraphs to create an index
     * 
     * @param idxname the name of the index to create
     */
    public void createKeyIndex(String idxname) {
        createKeyIndex(idxname, Vertex.class);
    }
    
    public <T extends Element> void createKeyIndex(String idxname, Class <T> idxClass) {
        kigraph.createKeyIndex(idxname, idxClass);
    }

    /**
     * Helper function to set the creation date property of nodes
     * 
     * At one point in time this was sys:created_at, however OrientDB has some
     * problems with ":" characters in property names, so it is now sys_created_at
     * 
     * @param elem Element to set creation date of
     */
    protected void setElementCreateTime(Element elem) {
        setProperty(elem, "sys_created_at", new Date());
    }

    /**
     * Creates an edge only if it doesn't already exist
     * 
     * @param id identifier for the edge, not used by some underlying databases
     * @param outVertex source vertex
     * @param inVertex target vertex
     * @param edgeLabel label for the edge
     * @return newly created edge
     */
    public Edge createEdgeIfNotExist(Object id, Vertex outVertex, Vertex inVertex, String edgeLabel) {
        for (Edge e : outVertex.getEdges(Direction.OUT, edgeLabel)) {
            if (e.getVertex(Direction.IN).equals(inVertex)) return e;
        }
        Edge re = kigraph.addEdge(id,  outVertex, inVertex, edgeLabel);
        setElementCreateTime(re);
        return re;
    }


    /**
     * Helper function for {@link #createEdgeIfNotExist(Object, Vertex, Vertex, String)} that ignores the first argument
     * 
     * @param outVertex source vertex
     * @param inVertex target vertex
     * @param edgeLabel label for the edge
     * @return newly created edge
     */
    public Edge createEdgeIfNotExist(Vertex outVertex, Vertex inVertex, String edgeLabel) {
        return createEdgeIfNotExist(null, outVertex, inVertex, edgeLabel);
    }



    /**
     * Wrapper function for removing edges.
     * 
     * This really doesn't do much other than abstract away the actual graph,
     * which may allow us to integrate with other databases in the future.
     * Likewise it may also work for removing edges from indices if they exist.
     * 
     * @param e
     */
    public void removeEdge(Edge e) {
        kigraph.removeEdge(e);
    }


    /**
     * Method that creates an vertex with no properties other than
     * the type and created_at.
     * 
     * Entries are also placed into the type index, but that's it.
     * 
     * This function does not handle specific ids for nodes
     * 
     * @param vertexType type of vertex to create
     * @return
     */
    protected Vertex createNakedVertex(String vertexType) {
        Vertex node = kigraph.addVertex(null);
        node.setProperty("type", vertexType.toString());
        typeidx.put("type", vertexType.toString(), node);
        setElementCreateTime(node);
        return node;
    }

    /**
     * Checks an index for an element, if found, returns it. If not, create the element and add it to the index.
     * 
     * @param idcol the name of the column which contains the id
     * @param idval the value of the id to look up in the index
     * @param vertexType the type of vertex to create
     * @param index the index containing the elements
     * @return the existing vertex or a new vertex
     */
    protected Vertex getOrCreateVertexHelper(String idcol, Object idval, String vertexType, Index <Vertex> index) {
        Vertex node = null;
        Iterable<Vertex> results = index.get(idcol, idval);
        for (Vertex v : results) {
            node = v;
            break;
        }
        if (node == null) {
            node = createNakedVertex(vertexType);
            node.setProperty(idcol, idval);
            index.put(idcol, idval, node);
        }
        return node;
    }



    /**
     * Converts an int (aka unix timestamp) to a java.util.Date object
     * 
     * This function is trivial, but useful when used in conjunction with
     * {@link #propertyToDate(Object)}
     * 
     * @param i the time in seconds since the beginning of the epoch
     * @return a java.util.Date object
     */
    public Date propertyToDate(int i) {
        return new Date(i*1000L);
    }

    /**
     * Converts a long to a java.util.Date object
     * 
     * This function is trivial, but useful when used in conjunction with
     * {@link #propertyToDate(Object)}
     * 
     * @param l the time in milliseconds since the beginning of the epoch
     * @return a java.util.Date object
     */
    public Date propertyToDate(long l) {
        return new Date(l);
    }

    /**
     * Converts a formatted date stringto a date object
     * 
     * @deprecated handles older cases
     * @param s a string such as 2012-02-10T19:22:10+0000
     * @return a java.util.Date object
     */
    public Date propertyToDate(String s) {
        try {
            return dateFormatter.parse(s);
        } catch (ParseException e) {
            log.error("Parse exception parsing \"{}\" into Date object", s, e);
            return null;
        }
    }

    /**
     * Generic helper function for converting a property to a date.
     * 
     * This should do the proper detection of the format of the object
     * and make it into a java.util.Date object as required
     * 
     * @param o
     * @return a java.util.Date object
     */
    public Date propertyToDate(Object o) {
        if (o instanceof Integer)
            return propertyToDate(((Integer) o).intValue());
        else if (o instanceof Long)
            return propertyToDate(((Long) o).longValue());
        else if (o instanceof String)
            return propertyToDate((String) o);
        log.error("Unable to process object of class: {}", o.getClass());
        return null;
    }

    /**
     * Simple helper function that subtracts d2 from d1
     * 
     * @deprecated
     * @param d1
     * @param d2
     * @return difference in days as a double
     */
    public double dateDifference(Date d1, Date d2) {
        double diff = (d1.getTime() - d2.getTime())/1000/86400;
        log.info("Date1: " + d1.getTime());
        log.info("Date2: " + d2.getTime());
        log.info("Difference: " + diff);
        return diff;
    }

    /**
     * Safety wrapper function for concluding a transaction
     * 
     * a logging warning is raised if the graph is not transactional
     */
    public void stopTransaction() {
        if (tgraph != null) {
            tgraph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } else {
            log.warn("Attempt to stop transaction on non-transactional graph");
        }
    }

    /**
     * Safety wrapper function for rolling back a transaction
     * 
     * a logging warning is raised if the graph is not transactional
     */
    public void rollbackTransaction() {
        if (tgraph != null) {
            tgraph.stopTransaction(TransactionalGraph.Conclusion.FAILURE);
        } else {
            log.warn("Attempt to rollback transaction on non-transactional graph");
        }
    }

    /**
     * Adds the object to the index only if it isn't already there
     * 
     * @param idcol the column of the index to reference
     * @param idval the value of the index to search for
     * @param object the object we want to make sure isn't there
     * @param index the index to operate on
     * @return false if the object is in the index already, true if not
     */
    protected <T extends Element> boolean addToIndexIfNotPresent(String idcol, Object idval, T object, Index<T> index) {
        for (T obj : index.get(idcol, idval)) {
            if (obj.equals(object)) return false;
        }
        index.put(idcol, idval, object);
        return true;
    }

    /**
     * Sets a string property on an element, ensures it is not null first
     * 
     * NOTE: this automatically trims 
     * @param elem Element to set the property
     * @param propname name of the property
     * @param property the value of the property
     */
    public void setProperty(Element elem, String propname, String property) {
        if (property != null && !property.trim().equals("")) elem.setProperty(propname, property.trim());
        log.trace("{} = {}", propname, property);
    }

    /**
     * Formats and sets a date property of an element
     * 
     * NOTE: dates are stored as UNIX timestamps. Thus we can lose
     * some precision here, but the extra space required for a LONG
     * really isn't useful here.
     * 
     * @param elem Element to set the property
     * @param propname name of the property
     * @param propdate date object to set
     */
    public void setProperty(Element elem, String propname, Date propdate) {
        if (propdate != null) {
            elem.setProperty(propname, propdate.getTime()/1000L);
        } else {
            log.trace("{} = null (not setting property)", propname);
        }
    }

    /**
     * Sets an integer property of an element
     * 
     * @param elem Element to set the property
     * @param propname name of the property
     * @param propvalue int value to set
     */
    public void setProperty(Element elem, String propname, int propvalue) {
        elem.setProperty(propname, propvalue);
        log.trace("{} = {}", propname, propvalue);
    }

    /**
     * Sets a long property of an element
     * 
     * @param elem Element to set the property
     * @param propname name of the property
     * @param propvalue long value to set
     */
    public void setProperty(Element elem, String propname, long propvalue) {
        elem.setProperty(propname, propvalue);
        log.trace("{} = {}", propname, propvalue);
    }	

    /**
     * Sets a double property of an element
     * 
     * @param elem Element to set the property
     * @param propname name of the property
     * @param propvalue double value to set
     */
    public void setProperty(Element elem, String propname, double propvalue) {
        elem.setProperty(propname, propvalue);
        log.trace("{} = {}", propname, propvalue);
    }

    /**
     * Sets a boolean property of an element
     * 
     * @param elem Element to set the property
     * @param propname name of the property
     * @param propvalue boolean value to set
     */
    public void setProperty(Element elem, String propname, boolean propvalue) {
        elem.setProperty(propname, propvalue);
        log.trace("{} = {}", propname, propvalue);
    }

    /**
     * Sets a generic property of an element
     * 
     * This should only get called if all of the other prototypes have been exhausted.
     * In which case there's a good chance that this method will raise an exception
     * as you can only use Java primatives.
     * 
     * @param elem Element to set the property
     * @param propname name of the property
     * @param propvalue object to set as value
     */
    public void setProperty(Element elem, String propname, Object propvalue) {
        if (propvalue != null) {
            elem.setProperty(propname, propvalue);
            log.trace("{} = {}", propname, propvalue);
        }
    }

    /**
     * Sets a property of an element if and only if that property is currently null
     * 
     * For example, if an element already has a property "BAR" and you try to set
     * a new value of "BAR" this will return false and not change the value. Usable
     * if you want to mimic immutable properties.
     * 
     * @param elem Element to set the property
     * @param key name of the property to set
     * @param value value of set
     * @return boolean on whether or not it was successful
     */
    protected <T extends Element> boolean setPropertyIfNull(T elem, String key, Object value) {
        if (elem.getProperty(key) != null) return false;
        elem.setProperty(key, value);
        log.trace("Setting key: {} = {}", key, value.toString());
        return true;
    }

    /* (non-Javadoc)
     * @see com.ibm.research.govsci.graph.Shutdownable#shutdown()
     */
    public void shutdown() {
        log.info("Shutting down graph database engine");
        kigraph.shutdown();
        log.trace("Graph shutdown complete");
    }
}
