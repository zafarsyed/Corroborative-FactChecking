package org.dice.FactCheck.Corraborative.Query;

import java.util.logging.Logger;

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.lang.sparql_11.ParseException;

public class QueryFormulator {

	public static void main(String[] args) {
		

	}
	
	public SelectBuilder formulateQuery(Triple firstTriple, Triple secondTriple, Node intermediateNode, SelectBuilder builder)
	{
		
		Node innerCount = NodeFactory.createVariable("b2");
		Node outerCount = NodeFactory.createVariable("b1");
		
		SelectBuilder selectBuilder = new SelectBuilder();
		try {
			selectBuilder = builder.clone().addVar("count(*)", outerCount).addVar(innerCount).addVar(firstTriple.getPredicate())
					.addVar(secondTriple.getPredicate())
					.addWhere(firstTriple)
					.addSubQuery(new SelectBuilder().
					addVar(secondTriple.getPredicate()).addVar("count(*)", innerCount).addVar(intermediateNode)
					.addWhere(secondTriple).addGroupBy(secondTriple.getPredicate()).addGroupBy(intermediateNode))
					.addGroupBy(firstTriple.getPredicate()).addGroupBy(secondTriple.getPredicate())
					.addGroupBy("?b2");
		} catch (ParseException e) {
			
			//
		}
		
		return selectBuilder;
	}

}
