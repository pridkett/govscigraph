/**
 * GraphShutdownHandler.java
 * 
 * Provides a handler that is invoked when the program terminates abnormally.
 * This should avoid various problems that you'ld other encounter with neo4j
 * when leaving a database open.
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
