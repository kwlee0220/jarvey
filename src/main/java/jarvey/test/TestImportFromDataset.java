package jarvey.test;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;


public class TestImportFromDataset {
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[2]")
											.getOrCreate();

		String dsId = "구역/연속지적도";
		Dataset<Row> ds = jarvey.read()
								.dataset(dsId);
		ds = ds.limit(50);
		
		ds.printSchema();
		ds.show(5);
		
		jarvey.spark().stop();
	}
}
