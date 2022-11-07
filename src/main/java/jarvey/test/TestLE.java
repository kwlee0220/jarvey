package jarvey.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestLE {
	public static final void main(String[] args) throws Exception {
		SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
		Dataset<Row> ds = spark.read().format("csv").load("/home/dna/tmp/yyy.csv");
		ds.show();
		
		spark.close();
	}
}