package jarvey.test;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;

public class TestExtent {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[2]")
											.getOrCreate();

		String dsId = "구역/읍면동";
//		String dsId = "교통/나비콜";
		
		SpatialDataFrame sdf = jarvey.read().dataset(dsId);
		
		sdf = sdf.agg(callUDF("ST_Extent", col("the_geom")).as("envelope"));
		sdf.persist();
		
		sdf.printSchema();
		sdf.show(5, false);
		
		jarvey.spark().stop();
	}
}
