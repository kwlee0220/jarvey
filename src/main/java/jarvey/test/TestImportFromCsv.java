package jarvey.test;

import org.apache.log4j.PropertyConfigurator;

import jarvey.JarveySession;
import jarvey.SpatialDataset;


public class TestImportFromCsv {
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_csv_files")
											.master("local[2]")
											.getOrCreate();
		
		SpatialDataset sds = jarvey.read().dataset("poi_buildings");
		sds.printSchema();
		
		sds.show(10);
		
		jarvey.spark().stop();
	}
}
