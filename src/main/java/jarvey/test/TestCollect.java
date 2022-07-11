package jarvey.test;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;

public class TestCollect {
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		String dsId = "구역/연속지적도";
		Dataset<Row> df = jarvey.read().dataset(dsId);
		df = df.limit(5);
		df.printSchema();
		df.show(5, true);
		
		df = df.agg(callUDF("ST_Collect", col("the_geom")).as("collect"));

		df.printSchema();
		df.show(5);
		
		jarvey.spark().stop();
	}
}
