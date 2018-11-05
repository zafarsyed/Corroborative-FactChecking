package org.dice.FactCheck.Corraborative;

import java.math.BigDecimal;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.Callable;
import java.math.BigDecimal;

import javax.print.DocFlavor.STRING;

import org.apache.commons.beanutils.converters.BigDecimalConverter;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.lang.sparql_11.ParseException;
import org.apache.jena.sparql.path.Path;
import org.apache.jena.sparql.path.PathFactory;
import org.apache.jena.vocabulary.RDF;

public class PMICalculator implements Callable<Result>{
	
	public String path;
	public Statement inputStatement;
	public int pathLength;
	public SelectBuilder builder;


	public PMICalculator(String path, SelectBuilder builder, Statement inputStatement, int pathLength) {
		this.path = path;
		this.builder = builder;
		this.inputStatement = inputStatement;
		this.pathLength = pathLength;
	}


	public double calculatePMIScore(String path, SelectBuilder builder, Statement input, int pathLength) throws ParseException
	{
	
		if(pathLength==3)
		{
			ParameterizedSparqlString query_Path_Occurrence = new ParameterizedSparqlString("SELECT (COUNT(*) as ?c) where \n"
					+this.builder.getWhereHandler().getElement());
					query_Path_Occurrence.setParam("p1", NodeFactory.createURI(path.split(";")[0]));
					query_Path_Occurrence.setParam("p2", NodeFactory.createURI(path.split(";")[1]));
					query_Path_Occurrence.setParam("p3", NodeFactory.createURI(path.split(";")[2]));
			QueryExecution qe1 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Path_Occurrence.asQuery());
			
			int count_Path_Occurrence = qe1.execSelect().next().get("?c").asLiteral().getInt();
			
			builder.addWhere(NodeFactory.createVariable("s"), input.getPredicate().asNode(), NodeFactory.createVariable("o"));
			ParameterizedSparqlString query_Path_Predicate_Occurrence = new ParameterizedSparqlString("SELECT (COUNT(*) as ?c) where \n"
					+builder.getWhereHandler().getElement());
			query_Path_Predicate_Occurrence.setParam("p1", NodeFactory.createURI(path.split(";")[0]));
			query_Path_Predicate_Occurrence.setParam("p2", NodeFactory.createURI(path.split(";")[1]));
			query_Path_Predicate_Occurrence.setParam("p3", NodeFactory.createURI(path.split(";")[2]));
			QueryExecution qe2 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Path_Predicate_Occurrence.asQuery());
			int count_path_Predicate_Occurrence = qe2.execSelect().next().get("?c").asLiteral().getInt();	
			
			SelectBuilder predicate_Occurrence = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			predicate_Occurrence.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			predicate_Occurrence.addVar("count(*)", "?c");
			predicate_Occurrence.addWhere(NodeFactory.createVariable("s"), input.getPredicate().asNode(), NodeFactory.createVariable("o"));
	
			Query query_Predicate_Occurrence = predicate_Occurrence.build();
			QueryExecution qe3 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Predicate_Occurrence);
			int count_predicate_Occurrence = qe3.execSelect().next().get("?c").asLiteral().getInt();
	
	
			SelectBuilder subject_Triples = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			subject_Triples.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			subject_Triples.addVar("count(?s)", "?c");
			subject_Triples.addWhere(NodeFactory.createVariable("x"), "^rdfs:domain", input.getPredicate().asNode());
			subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createVariable("x"));
			//subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Company"));
			//subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Person"));
	
			Query query_Subject_Triples = subject_Triples.build();
			QueryExecution qe4 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Subject_Triples);
			int count_subject_Triples = qe4.execSelect().next().get("?c").asLiteral().getInt();
	
			SelectBuilder object_Triples = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			object_Triples.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			object_Triples.addVar("count(?s)", "?c");
			object_Triples.addWhere(NodeFactory.createVariable("x"), "^rdfs:range", input.getPredicate().asNode());
			object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createVariable("x"));
			//object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Person"));
			//object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/University"));
	
			Query query_Object_Triples = object_Triples.build();
			QueryExecution qe5 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Object_Triples);
			int count_object_Triples = qe5.execSelect().next().get("?c").asLiteral().getInt();
	
			BigDecimal NO_OF_SUBJECT_TRIPLES = new BigDecimal(Integer.toString(count_subject_Triples));
			BigDecimal NO_OF_OBJECT_TRIPLES = new BigDecimal(Integer.toString(count_object_Triples));
			BigDecimal NO_PATH_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_path_Predicate_Occurrence));
			BigDecimal SUBJECT_OBJECT_TRIPLES = NO_OF_SUBJECT_TRIPLES.multiply(NO_OF_OBJECT_TRIPLES);
			double PROBABILITY_PATH_PREDICATE = NO_PATH_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, 
					RoundingMode.HALF_EVEN).doubleValue() + 0.0000000000000001;
			BigDecimal NO_PATH_TRIPLES = new BigDecimal(Integer.toString(count_Path_Occurrence));
			BigDecimal NO_OF_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_predicate_Occurrence));
			double PROBABILITY_PATH = NO_PATH_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();
			double PROBABILITY_PREDICATE = NO_OF_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();
			//System.out.println(path+" specificity "+(1/(1+PROBABILITY_PATH)));
						
			return (Math.log(PROBABILITY_PATH_PREDICATE/(PROBABILITY_PATH * PROBABILITY_PREDICATE))/-Math.log(PROBABILITY_PATH_PREDICATE));			
		}
	
		if(pathLength==2)
		{	
			ParameterizedSparqlString query_Path_Occurrence = new ParameterizedSparqlString("SELECT (COUNT(*) as ?c) where \n"
					+this.builder.getWhereHandler().getElement());
					query_Path_Occurrence.setParam("p1", NodeFactory.createURI(path.split(";")[0]));
					query_Path_Occurrence.setParam("p2", NodeFactory.createURI(path.split(";")[1]));
			QueryExecution qe1 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Path_Occurrence.asQuery());
			
			int count_Path_Occurrence = qe1.execSelect().next().get("?c").asLiteral().getInt();
			
			builder.addWhere(NodeFactory.createVariable("s"), input.getPredicate().asNode(), NodeFactory.createVariable("o"));
			ParameterizedSparqlString query_Path_Predicate_Occurrence = new ParameterizedSparqlString("SELECT (COUNT(*) as ?c) where \n"
					+builder.getWhereHandler().getElement());
			query_Path_Predicate_Occurrence.setParam("p1", NodeFactory.createURI(path.split(";")[0]));
			query_Path_Predicate_Occurrence.setParam("p2", NodeFactory.createURI(path.split(";")[1]));
			QueryExecution qe2 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Path_Predicate_Occurrence.asQuery());
			int count_path_Predicate_Occurrence = qe2.execSelect().next().get("?c").asLiteral().getInt();
	
			SelectBuilder predicate_Occurrence = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			predicate_Occurrence.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			predicate_Occurrence.addVar("count(*)", "?c");
			predicate_Occurrence.addWhere(NodeFactory.createVariable("s"), input.getPredicate().asNode(), NodeFactory.createVariable("o"));
	
			Query query_Predicate_Occurrence = predicate_Occurrence.build();
			QueryExecution qe3 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Predicate_Occurrence);
			int count_predicate_Occurrence = qe3.execSelect().next().get("?c").asLiteral().getInt();
	
	
			SelectBuilder subject_Triples = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			subject_Triples.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			subject_Triples.addVar("count(?s)", "?c");
			subject_Triples.addWhere(NodeFactory.createVariable("x"), "^rdfs:domain", input.getPredicate().asNode());
			subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createVariable("x"));
			//subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Company"));
			//subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Person"));
			
			Query query_Subject_Triples = subject_Triples.build();
			QueryExecution qe4 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Subject_Triples);
			int count_subject_Triples = qe4.execSelect().next().get("?c").asLiteral().getInt();
			count_subject_Triples = 1818074;
	
			SelectBuilder object_Triples = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			object_Triples.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			object_Triples.addVar("count(?s)", "?c");
			object_Triples.addWhere(NodeFactory.createVariable("x"), "^rdfs:range", input.getPredicate().asNode());
			object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createVariable("x"));
			//object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Person"));
			//object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/University"));
			
			Query query_Object_Triples = object_Triples.build();
			QueryExecution qe5 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Object_Triples);
			int count_object_Triples = qe5.execSelect().next().get("?c").asLiteral().getInt();
			count_object_Triples = 1818074;
	
			BigDecimal NO_OF_SUBJECT_TRIPLES = new BigDecimal(Integer.toString(count_subject_Triples));
			BigDecimal NO_OF_OBJECT_TRIPLES = new BigDecimal(Integer.toString(count_object_Triples));
			BigDecimal NO_PATH_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_path_Predicate_Occurrence));
			BigDecimal SUBJECT_OBJECT_TRIPLES = NO_OF_SUBJECT_TRIPLES.multiply(NO_OF_OBJECT_TRIPLES);
			SUBJECT_OBJECT_TRIPLES = SUBJECT_OBJECT_TRIPLES.subtract(NO_OF_OBJECT_TRIPLES);
			//SUBJECT_OBJECT_TRIPLES = new BigDecimal(Integer.toString(438336518));
			double PROBABILITY_PATH_PREDICATE = NO_PATH_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 25, RoundingMode.HALF_EVEN).doubleValue() + 0.0000000000001;
			BigDecimal NO_PATH_TRIPLES = new BigDecimal(Integer.toString(count_Path_Occurrence));
			BigDecimal NO_OF_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_predicate_Occurrence));
			double PROBABILITY_PATH = NO_PATH_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();
			double PROBABILITY_PREDICATE = NO_OF_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();
			//System.out.println(path+" specificity "+(1/(1+PROBABILITY_PATH)));
			
			return Math.log(PROBABILITY_PATH_PREDICATE/(PROBABILITY_PATH * PROBABILITY_PREDICATE))/-Math.log(PROBABILITY_PATH_PREDICATE);
			//return 0.0;
		}
	
	
	
		else if(pathLength==1)
		{			
			ParameterizedSparqlString query_Path_Occurrence = new ParameterizedSparqlString("SELECT (COUNT(*) as ?c) where \n"
			+builder.getWhereHandler().getElement());
			query_Path_Occurrence.setParam("p", NodeFactory.createURI(path));
			QueryExecution qe1 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Path_Occurrence.asQuery());
			
			int count_Path_Occurrence = qe1.execSelect().next().get("?c").asLiteral().getInt();
			
			builder.addWhere(NodeFactory.createVariable("s"), input.getPredicate().asNode(), NodeFactory.createVariable("o"));
			ParameterizedSparqlString query_Predicate_Path_Occurrence = new ParameterizedSparqlString("SELECT (count(*) as ?c) where \n"
					+builder.getWhereHandler().getElement());
			
			query_Predicate_Path_Occurrence.setParam("p", NodeFactory.createURI(path));
			QueryExecution qe2 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Predicate_Path_Occurrence.asQuery());
			int count_path_Predicate_Occurrence = qe2.execSelect().next().get("?c").asLiteral().getInt();
	
			SelectBuilder predicate_Occurrence = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			predicate_Occurrence.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			predicate_Occurrence.addVar("count(*)", "?c");
			predicate_Occurrence.addWhere(NodeFactory.createVariable("s"), input.getPredicate().asNode(), NodeFactory.createVariable("o"));
	
			Query query_Predicate_Occurrence = predicate_Occurrence.build();
			QueryExecution qe3 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Predicate_Occurrence);
			int count_predicate_Occurrence = qe3.execSelect().next().get("?c").asLiteral().getInt();
	
	
			SelectBuilder subject_Triples = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			subject_Triples.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			subject_Triples.addVar("count(?s)", "?c");
			subject_Triples.addWhere(NodeFactory.createVariable("x"), "^rdfs:domain", input.getPredicate().asNode());
			subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createVariable("x"));
			//subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Company"));
			//subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Person"));
	
			Query query_Subject_Triples = subject_Triples.build();
			QueryExecution qe4 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Subject_Triples);
			int count_subject_Triples = qe4.execSelect().next().get("?c").asLiteral().getInt();
	
			SelectBuilder object_Triples = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			object_Triples.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			object_Triples.addVar("count(?s)", "?c");
			object_Triples.addWhere(NodeFactory.createVariable("x"), "^rdfs:range", input.getPredicate().asNode());
			object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createVariable("x"));
			//object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Person"));
			//object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/University"));
	
			Query query_Object_Triples = object_Triples.build();
			QueryExecution qe5 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://dbpedia-live.openlinksw.com/sparql/", query_Object_Triples);
			int count_object_Triples = qe5.execSelect().next().get("?c").asLiteral().getInt();
	
			BigDecimal NO_OF_SUBJECT_TRIPLES = new BigDecimal(Integer.toString(count_subject_Triples));
			BigDecimal NO_OF_OBJECT_TRIPLES = new BigDecimal(Integer.toString(count_object_Triples));
			BigDecimal NO_PATH_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_path_Predicate_Occurrence));
			BigDecimal SUBJECT_OBJECT_TRIPLES = NO_OF_SUBJECT_TRIPLES.multiply(NO_OF_OBJECT_TRIPLES);
			//SUBJECT_OBJECT_TRIPLES = SUBJECT_OBJECT_TRIPLES.subtract(NO_OF_OBJECT_TRIPLES);
			//SUBJECT_OBJECT_TRIPLES = new BigDecimal(Integer.toString(438336518));
			double PROBABILITY_PATH_PREDICATE = NO_PATH_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue() + 0.0000000000000001;
			BigDecimal NO_PATH_TRIPLES = new BigDecimal(Integer.toString(count_Path_Occurrence));
			BigDecimal NO_OF_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_predicate_Occurrence));
			double PROBABILITY_PATH = NO_PATH_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();
			double PROBABILITY_PREDICATE = NO_OF_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();
			
			return Math.log(PROBABILITY_PATH_PREDICATE/(PROBABILITY_PATH * PROBABILITY_PREDICATE))/-Math.log(PROBABILITY_PATH_PREDICATE);
			//return 0.0;
			
		}
		
		else return 0.0;
	
	}


	public Result call() throws Exception {
		
		double score = calculatePMIScore(this.path, this.builder, this.inputStatement, this.pathLength);
		Result result = new Result(this.path, this.inputStatement.getPredicate(), score);
		return result;
		
	}

}
