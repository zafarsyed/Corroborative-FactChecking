package org.dice.FactCheck.Corraborative;

import java.math.BigDecimal;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.Callable;
import java.math.BigDecimal;

import javax.print.DocFlavor.STRING;

import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.apache.commons.beanutils.converters.BigDecimalConverter;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.lang.sparql_11.ParseException;
import org.apache.jena.sparql.path.Path;
import org.apache.jena.sparql.path.PathFactory;
import org.apache.jena.sparql.syntax.ElementVisitor;
import org.apache.jena.vocabulary.RDF;

import arq.query;

public class PMICalculator implements Callable<Result>{

	public String path;
	public Statement inputStatement;
	public int pathLength;
	public String builder;
	public int count_predicate_Occurrence;
	public int count_subject_Triples;
	public int count_object_Triples;
	public QueryExecutionFactoryHttp qef;


	public PMICalculator(String path, String builder, Statement inputStatement, int pathLength, int count_predicate_Occurrence, 
			int count_subject_Triples, int count_object_Triples, QueryExecutionFactoryHttp qef) {
		this.path = path;
		this.builder = builder;
		this.inputStatement = inputStatement;
		this.pathLength = pathLength;
		this.count_predicate_Occurrence = count_predicate_Occurrence;
		this.count_subject_Triples = count_subject_Triples;
		this.count_object_Triples = count_object_Triples;
		this.qef = qef;
	}


	public double calculatePMIScore() throws ParseException
	{

		if(pathLength==3)
		{
			String[] querySequence = builder.split(";");
			try {
				
				Triple firstPath = new Triple(NodeFactory.createVariable(querySequence[0].split(" ")[0].trim().replace("?", "")),
						NodeFactory.createURI(path.split(";")[0]), NodeFactory.createVariable(querySequence[0].split(" ")[2].trim().replace("?", "")));
				
				Triple secondPath = new Triple(NodeFactory.createVariable(querySequence[1].split(" ")[0].trim().replace("?", "")),
						NodeFactory.createURI(path.split(";")[1]), NodeFactory.createVariable(querySequence[1].split(" ")[2].trim().replace("?", "")));
				
				Triple thirdPath = new Triple(NodeFactory.createVariable(querySequence[2].split(" ")[0].trim().replace("?", "")),
						NodeFactory.createURI(path.split(";")[2]), NodeFactory.createVariable(querySequence[2].split(" ")[2].trim().replace("?", "")));
				
				SelectBuilder pathBuilder = new SelectBuilder().addVar("SUM(?x)", "sum").addSubQuery(new SelectBuilder().
						addVar("(?b3*?k)", "x").addSubQuery(new SelectBuilder().addVar("count(*)", "b3").addVar("(?b2*?b1)", "k").addWhere(firstPath).
								addSubQuery(new SelectBuilder().addVar("COUNT(*)", "b2").addVar("x1").addVar("b1").addWhere(secondPath).
								addSubQuery(new SelectBuilder().addVar("COUNT(*)", "b1").addVar("x2").addWhere(thirdPath)))));
				
				QueryExecution qe1 = qef.createQueryExecution(pathBuilder.build());
				double count_Path_Occurrence = qe1.execSelect().next().get("?sum").asLiteral().getDouble();
				qe1.close();
				
				Triple predicatePath = new Triple(NodeFactory.createVariable("s"), inputStatement.getPredicate().asNode(), NodeFactory.createVariable("o"));
				
				SelectBuilder predPathBuilder = new SelectBuilder().addVar("COUNT(*)", "c").addWhere(firstPath).addWhere(secondPath).addWhere(thirdPath).
						addWhere(predicatePath);
				
				QueryExecution qe2 = qef.createQueryExecution(predPathBuilder.build());

				int count_path_Predicate_Occurrence = qe2.execSelect().next().get("?c").asLiteral().getInt();	
				qe2.close();

				BigDecimal NO_OF_SUBJECT_TRIPLES = new BigDecimal(Integer.toString(count_subject_Triples));
				BigDecimal NO_OF_OBJECT_TRIPLES = new BigDecimal(Integer.toString(count_object_Triples));
				BigDecimal NO_PATH_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_path_Predicate_Occurrence));
				BigDecimal SUBJECT_OBJECT_TRIPLES = NO_OF_SUBJECT_TRIPLES.multiply(NO_OF_OBJECT_TRIPLES);
				double PROBABILITY_PATH_PREDICATE = NO_PATH_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, 
						RoundingMode.HALF_EVEN).doubleValue() + 0.0000000000000001;
				BigDecimal NO_PATH_TRIPLES = new BigDecimal(Double.toString(count_Path_Occurrence));
				BigDecimal NO_OF_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_predicate_Occurrence));
				double PROBABILITY_PATH = NO_PATH_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();
				double PROBABILITY_PREDICATE = NO_OF_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();

				return (Math.log(PROBABILITY_PATH_PREDICATE/(PROBABILITY_PATH * PROBABILITY_PREDICATE))/-Math.log(PROBABILITY_PATH_PREDICATE));	
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}

		if(pathLength==2)
		{	
			String[] querySequence = builder.split(";");
			try {
				Triple firstPath = new Triple(NodeFactory.createVariable(querySequence[0].split(" ")[0].trim().replace("?", "")),
						NodeFactory.createURI(path.split(";")[0]), NodeFactory.createVariable(querySequence[0].split(" ")[2].trim().replace("?", "")));
				
				Triple secondPath = new Triple(NodeFactory.createVariable(querySequence[1].split(" ")[0].trim().replace("?", "")),
						NodeFactory.createURI(path.split(";")[1]), NodeFactory.createVariable(querySequence[1].split(" ")[2].trim().replace("?", "")));
				
				SelectBuilder pathBuilder = new SelectBuilder().addVar("SUM(?x)", "sum").addSubQuery(new SelectBuilder().addVar("(?b1*?b2)", "x").
						addSubQuery(new SelectBuilder().addVar("COUNT(*)", "b2").addVar("b1").addWhere(firstPath).addSubQuery(new SelectBuilder().
								addVar("COUNT(*)", "b1").addVar("x1").addWhere(secondPath))));
				
				QueryExecution qe1 = qef.createQueryExecution(pathBuilder.build());
				double count_Path_Occurrence = qe1.execSelect().next().get("?sum").asLiteral().getDouble();
				qe1.close();
				
				Triple predicatePath = new Triple(NodeFactory.createVariable("s"), inputStatement.getPredicate().asNode(), NodeFactory.createVariable("o"));
				
				SelectBuilder predPathBuilder = new SelectBuilder().addVar("COUNT(*)", "c").addWhere(firstPath).addWhere(secondPath).addWhere(predicatePath);

				QueryExecution qe2 = qef.createQueryExecution(predPathBuilder.build());

				int count_path_Predicate_Occurrence = qe2.execSelect().next().get("?c").asLiteral().getInt();
				qe2.close();

				BigDecimal NO_OF_SUBJECT_TRIPLES = new BigDecimal(Integer.toString(count_subject_Triples));
				BigDecimal NO_OF_OBJECT_TRIPLES = new BigDecimal(Integer.toString(count_object_Triples));
				BigDecimal NO_PATH_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_path_Predicate_Occurrence));
				BigDecimal SUBJECT_OBJECT_TRIPLES = NO_OF_SUBJECT_TRIPLES.multiply(NO_OF_OBJECT_TRIPLES);
				SUBJECT_OBJECT_TRIPLES = SUBJECT_OBJECT_TRIPLES.subtract(NO_OF_OBJECT_TRIPLES);

				double PROBABILITY_PATH_PREDICATE = NO_PATH_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 25, RoundingMode.HALF_EVEN).doubleValue() + 0.0000000000001;
				BigDecimal NO_PATH_TRIPLES = new BigDecimal(Double.toString(count_Path_Occurrence));
				BigDecimal NO_OF_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_predicate_Occurrence));
				double PROBABILITY_PATH = NO_PATH_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();
				double PROBABILITY_PREDICATE = NO_OF_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();

				return Math.log(PROBABILITY_PATH_PREDICATE/(PROBABILITY_PATH * PROBABILITY_PREDICATE))/-Math.log(PROBABILITY_PATH_PREDICATE);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				return 0;
			}
		}



		else if(pathLength==1)
		{			
			
			Triple firstPath = new Triple(NodeFactory.createVariable(builder.split(" ")[0].trim().replace("?", "")),
					NodeFactory.createURI(path), NodeFactory.createVariable(builder.split(" ")[2].trim().replace("?", "")));
			
			SelectBuilder pathBuilder = new SelectBuilder().addVar("COUNT(*)", "c").addWhere(firstPath);
			
			QueryExecution qe1 = qef.createQueryExecution(pathBuilder.build());
			int count_Path_Occurrence = qe1.execSelect().next().get("?c").asLiteral().getInt();
			qe1.close();
			
			Triple predicatePath = new Triple(NodeFactory.createVariable("s"), inputStatement.getPredicate().asNode(), NodeFactory.createVariable("o"));
			
			SelectBuilder predPathBuilder = new SelectBuilder().addVar("COUNT(*)", "c").addWhere(firstPath).addWhere(predicatePath);
			
			QueryExecution qe2 = qef.createQueryExecution(predPathBuilder.build());
			int count_path_Predicate_Occurrence = qe2.execSelect().next().get("?c").asLiteral().getInt();
			qe2.close();


			BigDecimal NO_OF_SUBJECT_TRIPLES = new BigDecimal(Integer.toString(count_subject_Triples));
			BigDecimal NO_OF_OBJECT_TRIPLES = new BigDecimal(Integer.toString(count_object_Triples));
			BigDecimal NO_PATH_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_path_Predicate_Occurrence));
			BigDecimal SUBJECT_OBJECT_TRIPLES = NO_OF_SUBJECT_TRIPLES.multiply(NO_OF_OBJECT_TRIPLES);
			double PROBABILITY_PATH_PREDICATE = NO_PATH_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue() + 0.0000000000000001;
			BigDecimal NO_PATH_TRIPLES = new BigDecimal(Integer.toString(count_Path_Occurrence));
			BigDecimal NO_OF_PREDICATE_TRIPLES = new BigDecimal(Integer.toString(count_predicate_Occurrence));
			double PROBABILITY_PATH = NO_PATH_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();
			double PROBABILITY_PREDICATE = NO_OF_PREDICATE_TRIPLES.divide(SUBJECT_OBJECT_TRIPLES, 15, RoundingMode.HALF_EVEN).doubleValue();

			return Math.log(PROBABILITY_PATH_PREDICATE/(PROBABILITY_PATH * PROBABILITY_PREDICATE))/-Math.log(PROBABILITY_PATH_PREDICATE);

		}

		else return 0.0;

	}


	public Result call() throws Exception {

		double score = calculatePMIScore();
		Result result = new Result(this.path, this.inputStatement.getPredicate(), score);
		return result;

	}

}
