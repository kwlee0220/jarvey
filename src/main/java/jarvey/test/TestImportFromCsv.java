package jarvey.test;

import jarvey.JarveySession;
import jarvey.SpatialDataset;


public class TestImportFromCsv {
	public static final void main(String[] args) throws Exception {
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
