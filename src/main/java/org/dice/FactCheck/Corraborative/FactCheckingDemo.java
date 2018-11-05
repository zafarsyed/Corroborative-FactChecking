package org.dice.FactCheck.Corraborative;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactCheckingDemo {

	public static void main(String[] args) throws InterruptedException {
		
		org.apache.log4j.PropertyConfigurator.configure(FactChecking.class.getClassLoader().getResource("properties/log4j.properties"));
		FactChecking.init();
		FactChecking.checkFacts(getTestModel(), "oscars" , 5680);

	}
	
	public static Model getTestModel() {
        final Model model = ModelFactory.createDefaultModel();
        //model.read(FactCheckingDemo.class.getClassLoader().getResourceAsStream("Warren_Buffet.ttl"), null, "TURTLE");
        model.read(FactCheckingDemo.class.getClassLoader().getResourceAsStream("Synthetic_Oscars.nt"), null, "TURTLE");
        //model.read(FactCheckingDemo.class.getClassLoader().getResourceAsStream("Test.nt"), null, "N-TRIPLES");
        return model;
    }

}
