package org.dice.FactCheck.Corraborative.Query;

import java.util.HashMap;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.sparql.lang.sparql_11.ParseException;

/*  A class to generate sparql queries to find paths of varying lengths 
 */
public class SparqlQueryGenerator {

	public HashMap<SelectBuilder, Integer> sparqlQueries = new HashMap<SelectBuilder, Integer>();
	public Node subjectNode;
	public Node objectNode;

	public SparqlQueryGenerator() {
		
		this.subjectNode = NodeFactory.createVariable("s");
		this.objectNode = NodeFactory.createVariable("o");
	}

	public void GeneratorSparqlQueries(Statement input, int path_Length) throws ParseException
	{
		
		if(path_Length==1)
		{
			// Queries of path length contain a single predicate
			Node predicate = NodeFactory.createVariable("p");
			// To generate queries in both direction, we switch positions of subject and object
			Triple SUBJECT_OBJECT = new Triple(this.subjectNode, predicate, this.objectNode);
			Triple OBJECT_SUBJECT = new Triple(this.objectNode, predicate, this.subjectNode);

			// Generate a generic query using SelectBuilder
			SelectBuilder sb_Path_Length1 = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			sb_Path_Length1.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			sb_Path_Length1.addVar(predicate);			

			// Generate and collect queries by extending the generic query with necessary where condition, respectively
			sparqlQueries.put(sb_Path_Length1.clone().addWhere(SUBJECT_OBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length1.clone().addWhere(OBJECT_SUBJECT), path_Length);
		}

		else if (path_Length==2)
		{
			// For generating queries of path length 2, we need paths p1, p2 
			// connecting subject and object respectively to and an intermediate node x1
			// s ---> p1 ---> x1 ---> p2 ---> o
			Node subjectPredicate = NodeFactory.createVariable("p1");
			Node objectPredicate = NodeFactory.createVariable("p2");
			Node intermediateNode = NodeFactory.createVariable("x1"); 
			Triple INPUTSUBJECT_AS_SUBJECT = new Triple(this.subjectNode, subjectPredicate, intermediateNode);
			Triple INPUTSUBJECT_AS_OBJECT = new Triple(intermediateNode, subjectPredicate, this.subjectNode);
			Triple INPUTOBJECT_AS_SUBJECT = new Triple(this.objectNode, objectPredicate, intermediateNode);
			Triple INPUTOBJECT_AS_OBJECT = new Triple(intermediateNode, objectPredicate, this.objectNode);

			// Generate a generic query using SelectBuilder
			SelectBuilder sb_Path_Length2 = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			sb_Path_Length2.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			sb_Path_Length2.addVar(subjectPredicate);
			sb_Path_Length2.addVar(objectPredicate);
			
			// Generate and collect queries by extending the generic query with necessary where condition, respectively
			sparqlQueries.put(sb_Path_Length2.clone().addWhere(INPUTSUBJECT_AS_SUBJECT).addWhere(INPUTOBJECT_AS_OBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length2.clone().addWhere(INPUTSUBJECT_AS_SUBJECT).addWhere(INPUTOBJECT_AS_SUBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length2.clone().addWhere(INPUTSUBJECT_AS_OBJECT).addWhere(INPUTOBJECT_AS_OBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length2.clone().addWhere(INPUTSUBJECT_AS_OBJECT).addWhere(INPUTOBJECT_AS_SUBJECT), path_Length);
		}

		else if(path_Length==3)
		{
			// For generating queries of path length 2, we need paths p1, p2, p3 
		    // connecting subject and object respectively to and an intermediate node x1, x2
			// s ---> p1 ---> x1 ---> p2 ---> x2 ---> p3 ---> o
			Node subjectPredicate = NodeFactory.createVariable("p1");
			Node objectPredicate = NodeFactory.createVariable("p3");
			Node intermediatePredicate = NodeFactory.createVariable("p2");
			Node firstIntermediateNode = NodeFactory.createVariable("x1");
			Node secondIntermediateNode = NodeFactory.createVariable("x2");
			
			// combinations for subject and object
			Triple INPUTSUBJECT_AS_SUBJECT = new Triple(this.subjectNode, subjectPredicate, firstIntermediateNode);
			Triple INPUTSUBJECT_AS_OBJECT = new Triple(firstIntermediateNode, subjectPredicate, this.subjectNode);
			Triple INPUTOBJECT_AS_SUBJECT = new Triple(this.objectNode, objectPredicate, secondIntermediateNode);
			Triple INPUTOBJECT_AS_OBJECT = new Triple(secondIntermediateNode,objectPredicate, this.objectNode);

			// Intermediate nodes combinations
			Triple SUBJECT_OBJECT = new Triple(firstIntermediateNode, intermediatePredicate, secondIntermediateNode);
			Triple OBJECT_SUBJECT = new Triple(secondIntermediateNode, intermediatePredicate, firstIntermediateNode);

			SelectBuilder sb_Path_Length3 = new SelectBuilder().addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			sb_Path_Length3.addPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			sb_Path_Length3.addVar(subjectPredicate);
			sb_Path_Length3.addVar(intermediatePredicate);
			sb_Path_Length3.addVar(objectPredicate);
			
			sparqlQueries.put(sb_Path_Length3.clone().addWhere(INPUTSUBJECT_AS_SUBJECT).addWhere(SUBJECT_OBJECT).addWhere(INPUTOBJECT_AS_SUBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length3.clone().addWhere(INPUTSUBJECT_AS_SUBJECT).addWhere(SUBJECT_OBJECT).addWhere(INPUTOBJECT_AS_OBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length3.clone().addWhere(INPUTSUBJECT_AS_OBJECT).addWhere(SUBJECT_OBJECT).addWhere(INPUTOBJECT_AS_SUBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length3.clone().addWhere(INPUTSUBJECT_AS_OBJECT).addWhere(SUBJECT_OBJECT).addWhere(INPUTOBJECT_AS_OBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length3.clone().addWhere(INPUTSUBJECT_AS_SUBJECT).addWhere(OBJECT_SUBJECT).addWhere(INPUTOBJECT_AS_SUBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length3.clone().addWhere(INPUTSUBJECT_AS_SUBJECT).addWhere(OBJECT_SUBJECT).addWhere(INPUTOBJECT_AS_OBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length3.clone().addWhere(INPUTSUBJECT_AS_OBJECT).addWhere(OBJECT_SUBJECT).addWhere(INPUTOBJECT_AS_SUBJECT), path_Length);
			sparqlQueries.put(sb_Path_Length3.clone().addWhere(INPUTSUBJECT_AS_OBJECT).addWhere(OBJECT_SUBJECT).addWhere(INPUTOBJECT_AS_OBJECT), path_Length);
		}
	}

}
