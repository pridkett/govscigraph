/**
 * BlueprintsBase.java
 * 
 * This library was originally developed for a joint research
 * project with the University of Nebraska, Lincoln under terms
 * of the Joint Study Agreement between IBM and UNL. According
 * to the terms of that agreement this package is licensed under
 * the Apache Public License 2.0 and may be freely redistributed
 * according to that license.
 */
package com.ibm.research.govsci.graph;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.IndexableGraph;
import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.util.TransactionalGraphHelper;
import com.tinkerpop.blueprints.pgm.util.TransactionalGraphHelper.CommitManager;

public class BlueprintsBase {
	private Logger log = null;
	private static final int COMMITMGR_COMMITS = 2000;
	protected IndexableGraph graph = null;
	protected CommitManager manager = null;
	protected SimpleDateFormat dateFormatter = null;
	private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
	
	private static final String INDEX_TYPE = "type-idx";

	protected Index<Vertex> typeidx = null;
	
	public BlueprintsBase(String engine, String dburl) {
		log = LoggerFactory.getLogger(BlueprintsBase.class);
		String eng = engine.toLowerCase().trim();
		log.debug("Requested database: {} url: {}", eng, dburl);
		if (eng.equals("neo4j")) {
			log.info("Opening neo4j graph at: {}", dburl);
			graph = new Neo4jGraph(dburl);
		} else {
			log.error("Undefined database engine: {}", eng);
			System.exit(-1);
		}
		
		manager = TransactionalGraphHelper.createCommitManager((TransactionalGraph) graph, COMMITMGR_COMMITS);

		log.info("attempting to fetch index: {}", INDEX_TYPE);
		typeidx = getOrCreateIndex(INDEX_TYPE);

		dateFormatter = new SimpleDateFormat(DATE_FORMAT);
		dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
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
		log.trace("Getting index: {} type: {}", idxname, idxClass.toString());
		try {
			idx = graph.getIndex(idxname, idxClass);
		} catch (NullPointerException e) {
			log.error("Null pointer exception fetching index: {} {}", idxname, e);
		} catch (RuntimeException e) {
			log.debug("Runtime exception encountered getting index {}. Upgrade to newer version of blueprints.", idxname);
		}
		if (idx == null) {
			log.warn("Creating index {} for class {}", idxname, idxClass.toString());
			idx = graph.createManualIndex(idxname, idxClass);
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
	
	
	public Edge createEdgeIfNotExist(Object id, Vertex outVertex, Vertex inVertex, String edgeLabel) {
		for (Edge e : outVertex.getOutEdges(edgeLabel)) {
			if (e.getInVertex().equals(inVertex)) return e;
		}
		Edge re = graph.addEdge(id,  outVertex, inVertex, edgeLabel);
		re.setProperty("created_at", dateFormatter.format(new Date()));
		manager.incrCounter();
		return re;
	}
	public Edge createEdgeIfNotExist(Vertex outVertex, Vertex inVertex, String edgeLabel) {
		return createEdgeIfNotExist(null, outVertex, inVertex, edgeLabel);
	}
	
	
	/**
	 * Method that creates an vertex with no properties other than
	 * the type and created_at.
	 * 
	 * Entries are also placed into the type index, but that's it.
	 * 
	 * @param vertexType
	 * @return
	 */
	protected Vertex createNakedVertex(String vertexType) {
		Vertex node = graph.addVertex(null);
		node.setProperty("type", vertexType.toString());
		node.setProperty("created_at", dateFormatter.format(new Date()));
		typeidx.put("type", vertexType.toString(), node);
		manager.incrCounter();
		return node;
	}
	
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
	
	public void setProperty(Element elem, String propname, String property) {
		if (property != null && !property.trim().equals("")) elem.setProperty(propname, property);
		log.trace("{} = {}", propname, property);
	}
	public void setProperty(Element elem, String propname, Date propdate) {
		if (propdate != null) {
			elem.setProperty(propname, dateFormatter.format(propdate));
			log.trace("{} = {}", propname, dateFormatter.format(propdate));
		} else {
			log.trace("{} = null", propname);
		}
	}
	public void setProperty(Element elem, String propname, int propvalue) {
		elem.setProperty(propname, propvalue);
		log.trace("{} = {}", propname, propvalue);
	}
	public void setProperty(Element elem, String propname, long propvalue) {
		elem.setProperty(propname, propvalue);
		log.trace("{} = {}", propname, propvalue);
	}	
	public void setProperty(Element elem, String propname, double propvalue) {
		elem.setProperty(propname, propvalue);
		log.trace("{} = {}", propname, propvalue);
	}
	public void setProperty(Element elem, String propname, boolean propvalue) {
		elem.setProperty(propname, propvalue);
		log.trace("{} = {}", propname, propvalue);
	}
	public void setProperty(Element elem, String propname, Object propvalue) {
		if (propvalue != null) {
			elem.setProperty(propname, propvalue);
			log.trace("{} = {}", propname, propvalue);
		}
	}

	protected <T extends Element> boolean setPropertyIfNull(T elem, String key, Object value) {
		if (elem.getProperty(key) != null) return false;
		elem.setProperty(key, value);
		log.trace("Setting key: {} = {}", key, value.toString());
		return true;
	}
	
	public void shutdown() {
		log.info("Shutting down graph database engine");
		manager.close();
		graph.shutdown();
	}

}
