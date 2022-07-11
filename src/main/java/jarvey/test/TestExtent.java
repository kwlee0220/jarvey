package jarvey.test;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import jarvey.JarveySession;
import jarvey.type.EnvelopeValue;

public class TestExtent {
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		EnvelopeValue.ENCODER.schema().printTreeString();

		String file = "hdfs://master01:9000/user/dna/data/나비콜/택시로그";
		StructType schema = DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("carno", DataTypes.StringType, false),	
			DataTypes.createStructField("ts", DataTypes.StringType, false),	
			DataTypes.createStructField("month", DataTypes.ByteType, false),	
			DataTypes.createStructField("sid_cd", DataTypes.StringType, false),	
			DataTypes.createStructField("besselX", DataTypes.IntegerType, false),	
			DataTypes.createStructField("besselY", DataTypes.IntegerType, false),	
			DataTypes.createStructField("status", DataTypes.ByteType, false),	
			DataTypes.createStructField("company", DataTypes.StringType, false),	
			DataTypes.createStructField("driver_id", DataTypes.StringType, false),	
			DataTypes.createStructField("xpos", DataTypes.DoubleType, false),	
			DataTypes.createStructField("ypos", DataTypes.DoubleType, false),	
		});
		Dataset<Row> df = jarvey.read()
				.option("comment", "#")
				.schema(schema)
				.csv(file)
				.withColumn("ts", to_timestamp(col("ts"), "yyyyMMddHHmmss"))
				.withColumn("the_geom", callUDF("ST_Point", col("xpos"), col("ypos")))
				.drop("xpos", "ypos", "besselX", "besselY")
				.limit(5);
		df = df.select("the_geom", "carno");
		df.persist();
		
		df.printSchema();
		df.show(5, true);
		
		df = df.agg(callUDF("ST_Extent", col("the_geom")).as("envelope"));
//		df = df.agg(callUDF("ST_Extent2", col("the_geom")));

		df.printSchema();
		df.show(5);
		
		jarvey.spark().stop();
	}
}
