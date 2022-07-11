package jarvey.test;

import static jarvey.jarvey_functions.spatial;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import jarvey.JarveySession;
import jarvey.SpatialDataset;


public class TestImportFromShapefile {
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[2]")
											.getOrCreate();

//		String file = "seoul_subway";
//		String file = "sbdata/공공데이터포털/연속지적도_2017";
//		String file = "sbdata/사업단자료/노인복지시설_통합";
//		String file = "sbdata/사업단자료/인구밀도_2000";
		String file = "sbdata/사업단자료/행정동코드"; 
		Dataset<Row> df = jarvey.read()
								.option("charset", "euc-kr")
//								.option("srid", "5181")
								.shapefile(file);
//		df = df.limit(100);
//		
//		df.printSchema();
//		df.show(5);
		
		spatial(jarvey, df, null).writeSpatial().mode(SaveMode.Overwrite).dataset("test");
		
		SpatialDataset sds = jarvey.read().dataset("test");
		sds.printSchema();
		sds.show(5);
		jarvey.spark().stop();
	}
}
