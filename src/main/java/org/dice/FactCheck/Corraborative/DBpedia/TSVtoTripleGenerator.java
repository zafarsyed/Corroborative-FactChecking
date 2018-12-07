package org.dice.FactCheck.Corraborative.DBpedia;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.StringTokenizer;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;



public class TSVtoTripleGenerator {

	public static void tsvToTriple(File sourcetsv, File outputTriple) throws IOException
	{
		Model model = ModelFactory.createDefaultModel();
		StringTokenizer st;
		BufferedReader TSVFile = new BufferedReader(new FileReader(sourcetsv));
		String line = TSVFile.readLine();
		int i =1;
		while(line!=null)
		{
			st = new StringTokenizer(line, "\t");
			Statement statement = ResourceFactory.createStatement(ResourceFactory.createResource("http://dbpedia.org/resource/"
					+st.nextToken().split(":")[1].replaceAll(">", "")), ResourceFactory.createProperty("http://dbpedia.org/ontology/"
					+st.nextToken().split(":")[1].replaceAll(">", "")), ResourceFactory.createResource("http://dbpedia.org/resource/"
							+st.nextToken().split(":")[1].replaceAll(">", "")));
			model.add(statement);
			line = TSVFile.readLine();
		}
		model.write(new FileOutputStream(outputTriple), "N-TRIPLES");
		
	}
	
	public static void main(String[] args) throws IOException {
		
		tsvToTriple(new File("/home/datascienceadmin/Downloads/dbpedia.3.8.tsv"), new File("/home/datascienceadmin/Downloads/dbpedia.3.8.ttl"));

	}

}
