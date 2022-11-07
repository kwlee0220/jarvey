package jarvey.test;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;


public class TestImportFromDataset {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.getOrCreate();

		String dsId = "구역/연속지적도";
		SpatialDataFrame ds = jarvey.read()
								.dataset(dsId);
		ds = ds.limit(50);
		
		ds.printSchema();
		ds.show(5);
		
		jarvey.spark().stop();
	}
}
