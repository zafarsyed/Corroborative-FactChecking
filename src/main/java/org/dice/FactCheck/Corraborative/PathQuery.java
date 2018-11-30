package org.dice.FactCheck.Corraborative;

import java.util.HashMap;

import org.apache.jena.arq.querybuilder.SelectBuilder;

/*
 * A data structure to generate paths and remember the directions in graph that lead to the path
 * and also the path length
 */

public class PathQuery {
	
	
	// A data structure to generate paths and remember the directions in graph that lead to the path
	private HashMap<String, String> pathBuilder = new HashMap<String, String>();
	
	private int pathLength;

	
	public PathQuery(HashMap<String, String> pathBuilder, int pathLength) {
		this.pathBuilder = pathBuilder;
		this.pathLength = pathLength;
	}

	public HashMap<String, String> getPathBuilder() {
		return this.pathBuilder;
	}

	public void setPathBuilder(HashMap<String, String> pathBuilder) {
		this.pathBuilder = pathBuilder;
	}

	public int getPathLength() {
		return this.pathLength;
	}

	public void setPathLength(int pathLength) {
		this.pathLength = pathLength;
	}
	
}
