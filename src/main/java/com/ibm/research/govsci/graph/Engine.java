package com.ibm.research.govsci.graph;

public class Engine {
    
    /**
     * protected constructor to prevent instantiation 
     */
    protected Engine() {}
    
    /**
     * used for normal neo4j graphs
     */
    public static final String NEO4J = "neo4j";
    
    /**
     * used for orientdb
     */
    public static final String ORIENTDB = "orientdb";
    
    /**
     * used for tinkergraphs (generally for in-memory operations)
     */
    public static final String TINKERGRAPH = "tinkergraph";
    
    /**
     * used for titan (distributed graph database)
     */
    public static final String TITAN = "titan";
    
    /**
     * used for rexster (REST enabled graph databases)
     */
    public static final String REXSTER = "rexster";
    
    /**
     * used for neo4j in batch mode (limited features)
     */
    public static final String NEO4JBATCH = "neo4jbatch";
}
