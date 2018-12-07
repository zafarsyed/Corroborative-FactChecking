package org.dice.FactCheck.Corraborative;


import java.io.FileNotFoundException;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.lang.sparql_11.ParseException;
import org.dice.FactCheck.Corraborative.FactChecking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactCheckingDemo {

	public static void main(String[] args) throws InterruptedException, FileNotFoundException, ParseException {
		
		org.apache.log4j.PropertyConfigurator.configure(FactChecking.class.getClassLoader().getResource("properties/log4j.properties"));
		FactChecking.init();
		FactChecking.checkFacts(getTestModel(), "wb" , 1001);

	}
	
	public static Model getTestModel() {
        final Model model = ModelFactory.createDefaultModel();
        //model.read(FactCheckingDemo.class.getClassLoader().getResourceAsStream("Warren_Buffet.ttl"), null, "TURTLE");
        //System.out.println(FactCheckingDemo.class.getClassLoader().getResourceAsStream("Synthetic_US_Vice_President.nt"));
        model.read(FactCheckingDemo.class.getClassLoader().getResourceAsStream("Warren_Buffet.nt"), null, "N-TRIPLES");
        //model.read(FactCheckingDemo.class.getClassLoader().getResourceAsStream("Test.nt"), null, "N-TRIPLES");
        return model;
    }

}