package jarvey.test;

import static jarvey.jarvey_functions.spatial;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;

public class TestBuildTemporalPoint {
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		String dsId = "나비콜/택시로그";
		Dataset<Row> df = jarvey.read().dataset(dsId);

		df = df.withColumn("day", to_date(col("ts")))
				.groupBy(col("carno"), col("driver_id"), col("day"))
					.agg(callUDF("TP_BuildTemporalPoint", col("the_geom"), col("ts")).as("location"));
		df.printSchema();
		spatial(jarvey, df, null).writeSpatial().dataset("나비콜/일별로그");
		
		jarvey.spark().stop();
	}
}
