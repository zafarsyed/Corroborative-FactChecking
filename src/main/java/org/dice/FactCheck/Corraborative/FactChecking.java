package org.dice.FactCheck.Corraborative;

import java.io.File;
import java.io.FileNotFoundException;
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

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
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

	public static void checkFacts(Model model, String dataset, int size) throws InterruptedException
	{
		final Logger LOGGER = LoggerFactory.getLogger(FactChecking.class);
		SparqlQueryGenerator query = new SparqlQueryGenerator();
		int i= 1001;
		for(; i<=size; i++)
		{
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

			for (int j=1; j<=3; j++)
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
			
			for (Entry<SelectBuilder, Integer> entry : query.sparqlQueries.entrySet()) {

				pathGenerators.add(new PathGenerator(entry.getKey(), inputStatement, entry.getValue()));
			}

			try
			{
				ExecutorService executor = Executors.newCachedThreadPool();

				for (Future<PathQuery> result  : executor.invokeAll(pathGenerators))
				{
					pathQueries.add(result.get());
				}

				executor.shutdown();
			}

			catch (Exception e) {
				e.printStackTrace();
			}
			//System.out.println("Finished collecting Queries");
			Set<PMICalculator> pmiCallables = new HashSet<PMICalculator>();
			Set<Result> results = new HashSet<Result>();
			for (PathQuery pathQuery : pathQueries)
			{
				for (Entry<String, SelectBuilder> entry : pathQuery.getPathBuilder().entrySet()) {
					//System.out.println(entry.getKey());
					pmiCallables.add(new PMICalculator(entry.getKey(), entry.getValue(), inputStatement, pathQuery.getPathLength()));
					i++;
					if(i>25)
						break;
				}
			}
			try
			{
				ExecutorService executor = Executors.newCachedThreadPool();

				for (Future<Result> result  : executor.invokeAll(pmiCallables))
				{
					results.add(result.get());
				}

				executor.shutdown();
			}

			catch (Exception e) {
				e.printStackTrace();
			}
			double negativeScore = 0.0;
			int negativeCounter = 0;
			for (Result result : results) {
				//System.out.println(result.path +" "+ result.score);
				if(result.score>0.25)
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
				System.out.println(stmt+" has truth value "+score/counter);
			else
				System.out.println(stmt+" has truth value "+negativeScore/negativeCounter);
			
			Thread.sleep(2000);

		}
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
