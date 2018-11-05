package org.dice.FactCheck.Corraborative;

import org.apache.jena.rdf.model.Property;

public class Result {
	
	public String path;
	public Property predicate;	
	public double score;
	public String pathSpecificity;
	
	public String getPathSpecificity() {
		return pathSpecificity;
	}


	public void setPathSpecificity(String pathSpecificity) {
		this.pathSpecificity = pathSpecificity;
	}


	public Result(String path, Property predicate, double score) {
		this.path = path;
		this.predicate = predicate;
		this.score = score;
	}


	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Property getPredicate() {
		return predicate;
	}

	public void setPredicate(Property predicate) {
		this.predicate = predicate;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

}
