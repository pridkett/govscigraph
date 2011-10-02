/**
 * BlueprintsBase.java
 * 
 * This library was originally developed for a joint research
 * project with the University of Nebraska, Lincoln under terms
 * of the Joint Study Agreement between IBM and UNL.
 *
 * Copyright (c) 2011 IBM Corporation
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
import java.util.HashSet;
import java.util.Set;
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
import com.tinkerpop.blueprints.pgm.impls.rexster.RexsterGraph;
import com.tinkerpop.blueprints.pgm.impls.tg.TinkerGraph;

public class BlueprintsBase implements Shutdownable {
	private Logger log = null;
	protected IndexableGraph graph = null;
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
		} else if (eng.equals("rexster")) {
			log.info("Opening rexster graph at: {}", dburl);
			graph = new RexsterGraph(dburl);
		} else if (eng.equals("tinkergraph")) {
			graph = new TinkerGraph();
		} else {
			log.error("Undefined database engine: {}", eng);
			System.exit(-1);
		}
		
		log.debug("attempting to fetch index: {}", INDEX_TYPE);
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

	/**
	 * Helper function to get Edge indexes
	 * 
	 * @param idxname the name of the index to retrieve
	 * @return the index if it exists, or a new index if it does not
	 */
	public Index<Edge> getOrCreateEdgeIndex(String idxname) {
		return (Index<Edge>)getOrCreateIndex(idxname, Edge.class);
	}
	
	public Edge createEdgeIfNotExist(Object id, Vertex outVertex, Vertex inVertex, String edgeLabel) {
		for (Edge e : outVertex.getOutEdges(edgeLabel)) {
			if (e.getInVertex().equals(inVertex)) return e;
		}
		Edge re = graph.addEdge(id,  outVertex, inVertex, edgeLabel);
		re.setProperty("sys:created_at", dateFormatter.format(new Date()));
		return re;
	}
	public Edge createEdgeIfNotExist(Vertex outVertex, Vertex inVertex, String edgeLabel) {
		return createEdgeIfNotExist(null, outVertex, inVertex, edgeLabel);
	}
	public Edge createEdgeIfNotExist(Object id, Vertex outVertex, Vertex inVertex, StringableEnum labelType) {
		return createEdgeIfNotExist(id, outVertex, inVertex, labelType.toString());		
	}
	public Edge createEdgeIfNotExist(Vertex outVertex, Vertex inVertex, StringableEnum labelType) {
		return createEdgeIfNotExist(null, outVertex, inVertex, labelType.toString());
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
		node.setProperty("sys:created_at", dateFormatter.format(new Date()));
		typeidx.put("type", vertexType.toString(), node);
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
	
	protected Vertex getOrCreateVertexHelper(String idcol, Object idval, StringableEnum vertexType, Index <Vertex> index) {
		return getOrCreateVertexHelper(idcol, idval, vertexType.toString(), index);
	}
	
	/**
	 * Helper function that gets all of the vertices of a particular type from
	 * the database provided they have not been updated in age days.
	 * 
	 * Vertices that lack a last_updated parameter are always returned
	 * 
	 * FIXME: right now this does NOT use indexes
	 * FIXME: is this function EVER used? Maybe deprecate?
	 * 
	 * @deprecated
	 * @param age number of days since last_updated
	 * @param idxname the name of the index to use (currently ignored)
	 * @param vtxtype the type of vertex to examine
	 * @param namefield the name of the field to return in the set
	 * @return
	 */
	public Set<String> getVertexHelper(double age, String idxname, StringableEnum vtxtype, String fieldname) {
		Set<String> s = new HashSet<String>();
		// FIXME: How do we get all of the values from an index?
		// Right now we iterate over all of the nodes, which is CRAPTASTIC
		for (Vertex vtx: graph.getVertices()) {
			Set<String> props = vtx.getPropertyKeys();
			if (props.contains("type") && vtx.getProperty("type").equals(vtxtype)
					&& props.contains("username")) {
				try {
					if (!props.contains("last_updated") ||
							dateDifference(new Date(), dateFormatter.parse((String)vtx.getProperty("last_updated"))) > age)
						s.add((String)vtx.getProperty(fieldname));
				} catch (ParseException e) {
					log.info("Error parsing date: " + vtx.getProperty("last_updated"));
				}
			}
		}
		return s;
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
	 * Passthrough function for setting the transaction buffer size
	 * 
	 * This function does a naked cast to a TransactionalGraph, so I'd imagine that
	 * this could cause some problems later down the line. For right now on
	 * neo4j and rexster we should be fine.
	 * 
	 * @param bufferSize the number of modifications allowed between commits
	 */
	public void setMaxBufferSize(int bufferSize) {
		if (bufferSize == 0) {
			log.warn("Transaction Buffer size set to 0, transactions will be MANUAL");
		} else {
			log.debug("Transaction buffer size set to {}", bufferSize);
		}
		((TransactionalGraph)graph).setMaxBufferSize(bufferSize);
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
			log.trace("{} = null (not setting property)", propname);
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
		graph.shutdown();
		log.trace("Graph shutdown complete");
	}

}
