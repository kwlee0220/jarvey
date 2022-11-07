package jarvey.test;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;

public class TestBuildTemporalPoint {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		String dsId = "나비콜/택시로그";
		SpatialDataFrame sdf = jarvey.read().dataset(dsId);

		Dataset<Row> df = sdf.withRegularColumn("day", to_date(col("ts")))
								.groupBy(col("carno"), col("driver_id"), col("day"))
									.agg(callUDF("TP_BuildTemporalPoint", col("the_geom"), col("ts")).as("location"));
		df.printSchema();
		jarvey.toSpatial(df).writeSpatial().dataset("나비콜/일별로그");
		
		jarvey.spark().stop();
	}
}
