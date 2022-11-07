package jarvey.test;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;

public class TestCollect {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();

		String dsId = "POI/주유소_가격";
//		String dsId = "구역/연속지적도";
		
		SpatialDataFrame sdf = jarvey.read().dataset(dsId);
		sdf = sdf.limit(5);
		sdf.printSchema();
		sdf.show(5, true);
		
		Dataset<Row> df = sdf.toDataFrame().agg(callUDF("ST_Collect", col("the_geom")).as("collect"));

		df.printSchema();
		df.show(5);
		
		jarvey.spark().stop();
	}
}
