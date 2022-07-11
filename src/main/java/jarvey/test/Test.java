package jarvey.test;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;

public class Test {
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[2]")
											.getOrCreate();
		
//		Dataset<Row> df = jarvey.read().dataset("traffic/dtg/2018");
//		Dataset<Row> df = jarvey.read().dataset("traffic/navi_call/taxi_logs");
//		Dataset<Row> df = jarvey.read().dataset("address/building_poi");
//		Dataset<Row> df = jarvey.read().dataset("토지/표준공시지가");
		Dataset<Row> df = jarvey.read().dataset("지오비전/유동인구/2015/월별_시간대");
		df.printSchema();
		df.show(5);
//		System.out.println(df.count());
		
		jarvey.spark().stop();
	}
}