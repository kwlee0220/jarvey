package jarvey.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;

public class Test3 {
	private static final Logger s_logger = LoggerFactory.getLogger(Test3.class);
	private static final String ENVL4326 = SpatialDataFrame.COLUMN_ENVL4326;
	
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		Dataset<Row> rows = jarvey.read().dataset("tmp/dtg_tpoints").toDataFrame();
		rows = rows.selectExpr("tpoint.first_ts", "tpoint.last_ts", "tpoint.length",
								"tpoint.last_ts-tpoint.first_ts as duration");
		rows.show(10);
			
		jarvey.spark().stop();
	}
}
