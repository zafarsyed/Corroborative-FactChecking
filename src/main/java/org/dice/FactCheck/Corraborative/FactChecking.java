package org.dice.FactCheck.Corraborative;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceF;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.lang.sparql_11.ParseException;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.dice.FactCheck.Corraborative.Query.SparqlQueryGenerator;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.andrewoma.dexx.collection.HashMap;



public class FactChecking {

	public static org.dice.FactCheck.Corraborative.Config.Config Config;

	public static void checkFacts(Model model, String dataset, int size) throws InterruptedException, FileNotFoundException, ParseException
	{
		Model outputModel = ModelFactory.createDefaultModel();
		final Logger LOGGER = LoggerFactory.getLogger(FactChecking.class);
		
		QueryExecutionFactoryHttp qef = new QueryExecutionFactoryHttp("http://131.234.29.111:8890/sparql");
		Property property = ResourceFactory.createProperty("http://dbpedia.org/ontology/profession");
		
		SelectBuilder predicate_Occurrence = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		predicate_Occurrence.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		predicate_Occurrence.addVar("count(*)", "?c");
		predicate_Occurrence.addWhere(NodeFactory.createVariable("s"), property, 
				NodeFactory.createVariable("o"));

		Query query_Predicate_Occurrence = predicate_Occurrence.build();
		//QueryExecution qe3 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://131.234.29.111:8890/sparql", query_Predicate_Occurrence);
		QueryExecution qe3 = qef.createQueryExecution(query_Predicate_Occurrence);
		int count_predicate_Occurrence = qe3.execSelect().next().get("?c").asLiteral().getInt();
		qe3.close();


		SelectBuilder subject_Triples = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		subject_Triples.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		subject_Triples.addVar("count(?s)", "?c");
		subject_Triples.addWhere(NodeFactory.createVariable("x"), "^rdfs:domain", property.asNode());
		subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createVariable("x"));
		//subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Company"));
		//subject_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Person"));

		Query query_Subject_Triples = subject_Triples.build();
		//QueryExecution qe4 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://131.234.29.111:8890/sparql", query_Subject_Triples);
		QueryExecution qe4 = qef.createQueryExecution(query_Subject_Triples);
		int count_subject_Triples = qe4.execSelect().next().get("?c").asLiteral().getInt();
		qe4.close();

		SelectBuilder object_Triples = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		object_Triples.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		object_Triples.addVar("count(?s)", "?c");
		//object_Triples.addWhere(NodeFactory.createVariable("x"), "^rdfs:range", property.asNode());
		//object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createVariable("x"));
		object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/Person"));
		//object_Triples.addWhere(NodeFactory.createVariable("s"), "rdf:type", NodeFactory.createURI("http://dbpedia.org/ontology/University"));

		Query query_Object_Triples = object_Triples.build();
		//QueryExecution qe5 = org.apache.jena.query.QueryExecutionFactory.sparqlService("http://131.234.29.111:8890/sparql", query_Object_Triples);
		QueryExecution qe5 = qef.createQueryExecution(query_Object_Triples);
		int count_object_Triples = qe5.execSelect().next().get("?c").asLiteral().getInt();
		qe5.close();
		
		
		
		int i= 1001;
		for(; i<=size; i++)
		{
			
			SparqlQueryGenerator query = new SparqlQueryGenerator();
			StmtIterator subjectIterator = model.listStatements(ResourceFactory.createResource("http://swc2017.aksw.org/task2/dataset/"+dataset+"-"+i), 
					RDF.subject, (RDFNode)null);
			RDFNode subject = subjectIterator.next().getObject();
			StmtIterator objectIterator = model.listStatements(ResourceFactory.createResource("http://swc2017.aksw.org/task2/dataset/"+dataset+"-"+i), 
					RDF.object, (RDFNode)null);
			RDFNode object = objectIterator.next().getObject();
			StmtIterator prediatetIterator = model.listStatements(ResourceFactory.createResource("http://swc2017.aksw.org/task2/dataset/"+dataset+"-"+i), 
					RDF.predicate, (RDFNode)null);
			RDFNode predicate = prediatetIterator.next().getObject();
			
			StmtIterator resourcetIterator = model.listStatements(ResourceFactory.createResource("http://swc2017.aksw.org/task2/dataset/"+dataset+"-"+i), 
					ResourceFactory.createProperty("http://swc2017.aksw.org/hasTruthValue"), (RDFNode)null);
			RDFNode stmt = resourcetIterator.next().getSubject();
			
			Statement inputStatement = ResourceFactory.createStatement(subject.asResource(), 
					ResourceFactory.createProperty(predicate.toString()), object);
			
			LOGGER.info("Checking Fact");
			double score = 0.0;
			int counter =0;			
			//Statement stmt = prediatetIterator.nextStatement();

			for (int j=1; j<=1; j++)
			{			
				try {

					query.GeneratorSparqlQueries(inputStatement, j);
				} catch (ParseException e) {
					LOGGER.info("Exception while generating Sparql queries. Probably due to parse exception");
				}
			}

			// Generate paths in parallel. Create set of callables
			Set<PathGenerator> pathGenerators = new HashSet<PathGenerator>();
			Set<PathQuery> pathQueries =  new HashSet<PathQuery>();
			
			for (Entry<String, Integer> entry : query.sparqlQueries.entrySet()) {

				PathGenerator pg = new PathGenerator(entry.getKey(), inputStatement, entry.getValue(), qef);
				PathQuery pq = pg.returnQuery();
				pathQueries.add(pq);
			}

/*			try
			{
				ExecutorService executor = Executors.newFixedThreadPool(100);

				for (Future<PathQuery> result  : executor.invokeAll(pathGenerators))
				{
					pathQueries.add(result.get());
				}

				executor.shutdown();
			}

			catch (Exception e) {
				e.printStackTrace();
			}*/
			//System.out.println("Finished collecting Queries");
			Set<PMICalculator> pmiCallables = new HashSet<PMICalculator>();
			Set<Result> results = new HashSet<Result>();
			int k=0;
			for (PathQuery pathQuery : pathQueries)
			{
				for (Entry<String, String> entry : pathQuery.getPathBuilder().entrySet()) {
					PMICalculator pc = new PMICalculator(entry.getKey(), entry.getValue(), inputStatement, pathQuery.getPathLength(),
							count_predicate_Occurrence, count_subject_Triples, count_object_Triples, qef);
					double score1 = pc.calculatePMIScore();
					System.out.println("Score for path "+entry.getKey()+" is "+score1);
					Result result = new Result(entry.getKey(), inputStatement.getPredicate(), score1);
					results.add(result);
					k++;
					if(k>10)
						break;
				}
			}
/*			try
			{
				ExecutorService executor = Executors.newFixedThreadPool(100);

				for (Future<Result> result  : executor.invokeAll(pmiCallables))
				{
					results.add(result.get());
				}

				executor.shutdown();
			}

			catch (Exception e) {
				e.printStackTrace();
			}*/
			double negativeScore = 0.0;
			int negativeCounter = 0;
			for (Result result : results) {
				//System.out.println(result.path +" "+ result.score);
				if(result.score>0.35)
				{
					score += result.score;
					counter++;
				}
				else
				{
					negativeScore += result.score;
					negativeCounter++;
				}
			}
			if(counter!=0)
			{
				System.out.println(stmt+" has truth value "+score/counter);
				outputModel.addLiteral(stmt.asResource(), ResourceFactory.createProperty("http://swc2017.aksw.org/hasTruthValue"),
						score/counter);
			}
			else if (negativeCounter !=0 )
			{
				System.out.println(stmt+" has truth value "+negativeScore/negativeCounter);
				outputModel.addLiteral(stmt.asResource(), ResourceFactory.createProperty("http://swc2017.aksw.org/hasTruthValue"),
						negativeScore/negativeCounter);
			}
			else
			{
				System.out.println(stmt+" has truth value "+negativeScore);
				outputModel.addLiteral(stmt.asResource(), ResourceFactory.createProperty("http://swc2017.aksw.org/hasTruthValue"),
						negativeScore);
			}
			//if(i%5 == 0)
			//Thread.sleep(60000);
			qef.close();

		}
		
		outputModel.write(new FileOutputStream(new File("/home/datascienceadmin/Output/Synthetic_US_Vice_President_output_1.nt")),
				"N-TRIPLES");
	}

	public static void init(){

		try {

			if ( FactChecking.Config  == null )
				FactChecking.Config = new org.dice.FactCheck.Corraborative.Config.Config(new Ini(new File(FactChecking.class.getResource("/defacto.ini").getFile())));

		} catch (InvalidFileFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
