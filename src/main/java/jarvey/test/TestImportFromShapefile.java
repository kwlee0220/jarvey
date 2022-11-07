package jarvey.test;

import java.io.File;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;


public class TestImportFromShapefile {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[2]")
//											.datasetRoot(new Path("jarvey-hadoop.xml"), "datasets")
											.root(new File("/mnt/data/sbdata"))
											.getOrCreate();

//		String file = "seoul_subway";
//		String file = "sbdata/공공데이터포털/연속지적도_2017";
//		File file = new File("/mnt/data/sbdata/사업단자료/노인복지시설_통합");
//		String file = "sbdata/사업단자료/인구밀도_2000";
//		File file = new File("/mnt/data/sbdata/사업단자료/지오비전/집계구/2018");
		File file = new File("/mnt/data/sbdata/행자부/법정구역_5179/시군구"); 
		SpatialDataFrame sds = jarvey.read()
	//								.option("charset", "utf-8")
//									.option("charset", "euc-kr")
	//								.option("srid", "5181")
									.shapefile(file);
//		sds = sds.select("the_geom","SIG_CD").limit(100);
		
		sds.printSchema();
		sds.explain();
//		System.out.println(Arrays.asList(df.rdd().partitions()));
		
		sds.show(10);
		
//		spatial(jarvey, df, null).writeSpatial().mode(SaveMode.Overwrite).dataset("test");
//		
//		SpatialDataset sds = jarvey.read().dataset("test");
//		sds.printSchema();
//		sds.show(5);
		jarvey.spark().stop();
	}
}
