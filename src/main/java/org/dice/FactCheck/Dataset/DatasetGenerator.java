package org.dice.FactCheck.Dataset;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceF;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

public class DatasetGenerator {
	
	
	public static void generateDataset(File inputFile, File outputFile) throws IOException
	{
		Model model = ModelFactory.createDefaultModel();
		Model model2 = ModelFactory.createDefaultModel();
		String line= "";
		int i=1001;
		BufferedReader br = new BufferedReader( new FileReader(inputFile));
		br.readLine();
		while((line=br.readLine())!=null)
		{
			System.out.println(line);
			String[] fact = line.split("\t");
			Resource subject = ResourceFactory.createResource("http://dbpedia.org/resource/"+fact[1].split(":")[1]);
			Resource object = ResourceFactory.createResource("http://dbpedia.org/resource/"+fact[5].split(":")[1]);
			Property property = ResourceFactory.createProperty("http://dbpedia.org/ontology/", fact[3].split(":")[1]);
			Resource id = ResourceFactory.createResource("http://swc2017.aksw.org/task2/dataset/rw_deathplace-"+i++);
			model.add(id, RDF.type, RDF.Statement);
			//Statement statement = ResourceFactory.createStatement(subject, property, object);
			model.add(id, RDF.subject, subject);
			model.add(id, RDF.predicate, property);
			model.add(id, RDF.object, object);
			if(Integer.parseInt(fact[6])==1)
				model.addLiteral(id, ResourceFactory.createProperty("http://swc2017.aksw.org/", "hasTruthValue"), 1.0);
			else
				model.addLiteral(id, ResourceFactory.createProperty("http://swc2017.aksw.org/", "hasTruthValue"), 0.0);
			
			model2.add(subject, property, object);
		}
		
		model2.write(new BufferedWriter(new FileWriter(outputFile)),"N-TRIPLES");
	}

	public static void main(String[] args) throws IOException {
		
		generateDataset(new File("E:\\knowledgestream\\datasets\\synthetic\\cross_Movies_vs_Directors.tsv"), 
				new File("E:\\knowledgestream\\datasets\\synthetic\\Synthetic_Oscars_Input.nt"));

	}

}
