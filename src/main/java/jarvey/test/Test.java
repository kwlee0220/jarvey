package jarvey.test;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;

public class Test {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[2]")
											.getOrCreate();
		
//		Dataset<Row> df = jarvey.read().dataset("traffic/dtg/2018");
//		Dataset<Row> df = jarvey.read().dataset("traffic/navi_call/taxi_logs");
//		Dataset<Row> df = jarvey.read().dataset("address/building_poi");
//		Dataset<Row> df = jarvey.read().dataset("토지/표준공시지가");
		SpatialDataFrame sdf = jarvey.read().dataset("지오비전/유동인구/2015/월별_시간대");
		sdf.printSchema();
		sdf.show(5);
//		System.out.println(df.count());
		
		jarvey.spark().stop();
	}
}