package com.ibm.research.govsci.graph;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple handler that calls a set of routines when it is time to shutdown
 * the program. This is commonly used to ensure that graphs are left in a
 * consistent state.
 * 
 * @author Patrick Wagstrom <pwagstro@us.ibm.com>
 */
public class GraphShutdownHandler extends Thread {
	private Logger log = null;
	protected ArrayList<Shutdownable> elems = null;
	
	public GraphShutdownHandler() {
		super();
		log = LoggerFactory.getLogger(GraphShutdownHandler.class);
		elems = new ArrayList<Shutdownable>();
	}
	
	public void addShutdownHandler(Shutdownable sd) {
		log.trace("Addding shutdown handler: {}", sd);
		elems.add(sd);
	}
	
	@Override()
	public void run() {
		log.debug("Invoking Shutdown Handlers");
		for (Shutdownable sd : elems) {
			log.trace("Invoking shutdown handler: {}", sd);
			sd.shutdown();
		}
	}
	
}
