package org.dice.FactCheck.Corraborative;

import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.apache.jena.sparql.lang.sparql_11.ParseException;

/*
 * A class implementing callable to generate paths in parallel and returns PathQuery, a
 * a structure for realizing the path
 */

public class PathGenerator implements Callable<PathQuery>{

	public SelectBuilder queryBuilder;
	public Statement input;
	public int pathLength;
	HashMap<String, SelectBuilder> paths = new HashMap<String, SelectBuilder>();
	public PathQuery pathQuery;

	public PathGenerator(SelectBuilder queryBuilder, Statement input, int pathLength)
	{
		this.queryBuilder = queryBuilder;
		this.input = input;
		this.pathLength = pathLength;
	}

	public PathQuery call() throws Exception {
		return returnQuery(this.queryBuilder, this.input, this.pathLength);
	}

	public PathQuery returnQuery(SelectBuilder query, Statement input, int pathLength) throws ParseException
	{

		if(pathLength == 1)
		{
			SelectBuilder pathQuery = query.clone();
			pathQuery.addFilter("?p NOT IN (rdf:type, rdfs:label, rdfs:comment, <http://purl.org/dc/terms/subject>, "
					+ "<http://xmlns.com/foaf/0.1/homepage>, <http://dbpedia.org/ontology/wikiPageExternalLink>, "
					+ "<http://www.w3.org/2000/01/rdf-schema#seeAlso>, <http://dbpedia.org/ontology/wikiPageWikiLink>)");
			pathQuery.addFilter("?p != <"+input.getPredicate()+">");
			ParameterizedSparqlString paraPathQuery = new ParameterizedSparqlString(pathQuery.toString());
			paraPathQuery.setParam("s", input.getSubject());
			paraPathQuery.setParam("o", input.getObject());

			QueryExecution qe = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/",
					paraPathQuery.asQuery());
			ResultSet result = qe.execSelect();
			while(result.hasNext())
			{
				QuerySolution qs = result.next();
				paths.put(qs.get("?p").toString(), query);
			}
			this.pathQuery = new PathQuery(paths, pathLength);
		}

		else if(pathLength == 2)
		{
			SelectBuilder pathQuery = query.clone();
			pathQuery.addFilter("?p1 NOT IN (rdf:type, rdfs:label, rdfs:comment, <http://purl.org/dc/terms/subject>, "
					+ "<http://xmlns.com/foaf/0.1/homepage>, <http://dbpedia.org/ontology/wikiPageExternalLink>, "
					+ "<http://www.w3.org/2000/01/rdf-schema#seeAlso>, <http://dbpedia.org/ontology/wikiPageWikiLink>)");
			pathQuery.addFilter("?p2 NOT IN (rdf:type, rdfs:label, rdfs:comment, <http://purl.org/dc/terms/subject>, "
					+ "<http://xmlns.com/foaf/0.1/homepage>, <http://dbpedia.org/ontology/wikiPageExternalLink>, "
					+ "<http://www.w3.org/2000/01/rdf-schema#seeAlso>, <http://dbpedia.org/ontology/wikiPageWikiLink>)");

			String ontology = "\'http://dbpedia.org/ontology\'";
			pathQuery.addFilter("strstarts(str(?p1),"+ontology+")");
			pathQuery.addFilter("strstarts(str(?p2), "+ontology+")");
			pathQuery.addFilter("!ISLITERAL(?x1)");

			ParameterizedSparqlString paraPathQuery = new ParameterizedSparqlString(pathQuery.toString());
			paraPathQuery.setParam("s", input.getSubject());
			paraPathQuery.setParam("o", input.getObject());

			QueryExecution qe = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/",
					paraPathQuery.asQuery());
			ResultSet result = qe.execSelect();
			while(result.hasNext())
			{
				QuerySolution qs = result.next();
				paths.put(qs.get("?p1").toString()+";"+qs.get("?p2").toString(), query);
			}
			this.pathQuery = new PathQuery(paths, pathLength);
		}

		else if(pathLength == 3)
		{
			SelectBuilder pathQuery = query.clone();
			pathQuery.addFilter(NodeFactory.createVariable("x1")+" != <"+input.getObject().asNode()+">");
			pathQuery.addFilter(NodeFactory.createVariable("x2")+" != <"+input.getSubject().asNode()+">");
			pathQuery.addFilter("?x1 != ?x2");
			String ontology = "\'http://dbpedia.org/ontology\'";
			pathQuery.addFilter("strstarts(str(?p1),"+ontology+")");
			pathQuery.addFilter("strstarts(str(?p2), "+ontology+")");
			pathQuery.addFilter("strstarts(str(?p3), "+ontology+")");
			pathQuery.addFilter("?p1 NOT IN (<http://dbpedia.org/ontology/wikiPageWikiLink>, <http://dbpedia.org/ontology/type>,"
					+ "<http://dbpedia.org/ontology/thumbnail>, <http://dbpedia.org/ontology/wikiPageExternalLink>)");
			pathQuery.addFilter("?p2 NOT IN (<http://dbpedia.org/ontology/wikiPageWikiLink>, <http://dbpedia.org/ontology/type>, "
					+ "<http://dbpedia.org/ontology/thumbnail>, <http://dbpedia.org/ontology/wikiPageExternalLink>)");
			pathQuery.addFilter("?p3 NOT IN (<http://dbpedia.org/ontology/wikiPageWikiLink>, <http://dbpedia.org/ontology/type>, "
					+ "<http://dbpedia.org/ontology/thumbnail>, <http://dbpedia.org/ontology/wikiPageExternalLink>)");
			pathQuery.addFilter("!ISLITERAL(?x1)");
			pathQuery.addFilter("!ISLITERAL(?x2)");

			ParameterizedSparqlString paraPathQuery = new ParameterizedSparqlString(pathQuery.toString());
			paraPathQuery.setParam("s", input.getSubject());
			paraPathQuery.setParam("o", input.getObject());

			QueryExecution qe = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/",
					paraPathQuery.asQuery());
			ResultSet result = qe.execSelect();
			while(result.hasNext())
			{
				QuerySolution qs = result.next();
				paths.put(qs.get("?p1").toString()+";"+qs.get("?p2").toString()+";"+qs.get("?p3").toString(), query);
			}
			this.pathQuery = new PathQuery(paths, pathLength);
		}

		return this.pathQuery;
	}



}
