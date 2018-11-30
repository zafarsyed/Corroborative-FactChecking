package org.dice.FactCheck.Corraborative;

import java.util.HashMap;
import java.util.concurrent.Callable;

import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
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

	public String queryBuilder;
	public Statement input;
	public int pathLength;
	HashMap<String, String> paths = new HashMap<String, String>();
	public PathQuery pathQuery;
	public QueryExecutionFactoryHttp qef;
	public String ontology = "\'http://dbpedia.org/ontology\'";

	public PathGenerator(String queryBuilder, Statement input, int pathLength, QueryExecutionFactoryHttp qef)
	{
		this.queryBuilder = queryBuilder;
		this.input = input;
		this.pathLength = pathLength;
		this.qef = qef;
	}

	public PathQuery call() throws Exception {
		return returnQuery();
	}

	public PathQuery returnQuery() throws ParseException
	{

		if(pathLength == 1)
		{
			ParameterizedSparqlString paraPathQuery = new ParameterizedSparqlString("SELECT ?p1 where "
					+ "\n { \n"+queryBuilder+" . \n"
					+ "FILTER(?p1 != <"+input.getPredicate()+">)"+"\n"
					+ "FILTER(strstarts(str(?p),"+ontology+")) \n }");
			paraPathQuery.setParam("s", input.getSubject());
			paraPathQuery.setParam("o", input.getObject());
			
			QueryExecution qe = qef.createQueryExecution(paraPathQuery.asQuery());
			ResultSet result = qe.execSelect();
			
			while(result.hasNext())
			{
				QuerySolution qs = result.next();
				paths.put(qs.get("?p1").toString(), queryBuilder);
			}
			this.pathQuery = new PathQuery(paths, pathLength);
			qe.close();
		}

		else if(pathLength == 2)
		{
			
			String[] querySequence = queryBuilder.split(";");
			ParameterizedSparqlString paraPathQuery = new ParameterizedSparqlString("SELECT ?p1 ?p2 where \n"
					+ "{ \n "+querySequence[0]+"."+querySequence[1]+"."+"\n"
					+"FILTER(strstarts(str(?p1),"+ontology+"))"
					+"FILTER(strstarts(str(?p2),"+ontology+"))"
					+"FILTER(!ISLITERAL(?x1))"+"\n }");
			
			paraPathQuery.setParam("s", input.getSubject());
			paraPathQuery.setParam("o", input.getObject());

			QueryExecution qe = qef.createQueryExecution(paraPathQuery.asQuery());
			ResultSet result = qe.execSelect();
			
			while(result.hasNext())
			{
				QuerySolution qs = result.next();
				paths.put(qs.get("?p1").toString()+";"+qs.get("?p2").toString(), queryBuilder);
			}
			this.pathQuery = new PathQuery(paths, pathLength);
			qe.close();
		}

		else if(pathLength == 3)
		{
			
			String[] querySequence = queryBuilder.split(";");
			ParameterizedSparqlString paraPathQuery = new ParameterizedSparqlString("SELECT ?p1 ?p2 ?p3 where \n"
					+"{ \n"+querySequence[0]+".\n"+querySequence[1]+".\n"+querySequence[2]+".\n"
					+"FILTER(?x1 != <"+input.getObject().asNode()+">) \n"
					+"FILTER(?x2 != <"+input.getSubject().asNode()+">) \n"
					+"FILTER(?x1 != ?x2) \n"
					+"FILTER(strstarts(str(?p1),"+ontology+"))"
					+"FILTER(strstarts(str(?p2),"+ontology+"))"
					+"FILTER(strstarts(str(?p3),"+ontology+"))"
					+"FILTER(!ISLITERAL(?x1)) \n"
					+"FILTER(!ISLITERAL(?x2)) \n }");
			paraPathQuery.setParam("s", input.getSubject());
			paraPathQuery.setParam("o", input.getObject());
			QueryExecution qe = qef.createQueryExecution(paraPathQuery.asQuery());
			ResultSet result = qe.execSelect();
			int k=0;
			while(result.hasNext())
			{
				QuerySolution qs = result.next();
				paths.put(qs.get("?p1").toString()+";"+qs.get("?p2").toString()+";"+qs.get("?p3").toString(), queryBuilder);
				if(++k > 5)
					break;
			}
			this.pathQuery = new PathQuery(paths, pathLength);
			qe.close();
		}

		return this.pathQuery;
	}



}
