package jarvey.test;

import org.apache.log4j.PropertyConfigurator;

import jarvey.JarveySession;
import jarvey.support.typeexpr.JarveyTypeParser;

public class Test3 {
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		JarveyTypeParser.parseTypeExpr("Point(4326)");
		
		jarvey.spark().stop();
	}
}
