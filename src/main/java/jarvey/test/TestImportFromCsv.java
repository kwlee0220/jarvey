package jarvey.test;

import org.apache.hadoop.fs.Path;

import jarvey.FilePath;
import jarvey.JarveySession;
import jarvey.LfsPath;
import jarvey.SpatialDataFrame;

import utils.stream.FStream;


public class TestImportFromCsv {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_csv_files")
											.master("local[7]")
											.hadoopDatasetRoot(new Path("jarvey-hadoop.xml"), "jarvey")
											.getOrCreate();
		SpatialDataFrame sdf;
		
//		FilePath path = LfsPath.of("/mnt/data/sbdata/사업단자료/전국어린이보호구역");
		FilePath path = LfsPath.of("/mnt/data/sbdata/도로교통안전공단/DTG_201809");
//		sdf = jarvey.read().dataset(path);
		
//		String dsId = "POI/주유소_가격";
//		String dsId = "POI/어린이보호구역";
//		String dsId = "교통/나비콜";
//		String dsId = "교통/DTG/2016";
		String dsId = "교통/DTG/2018";
//		String dsId = "경제/지오비전/카드매출/2015/시간대";
//		SpatialDataFrame sdf = jarvey.read().dataset("행자부/도로명주소/건물_위치정보");
//		SpatialDataFrame sdf = jarvey.read().dataset("교통/나비콜/2016/01");
//		SpatialDataFrame sdf = jarvey.read().dataset("교통/dtg_m");
//		SpatialDataFrame sdf = jarvey.read().dataset("POI/주유소_가격");
//		SpatialDataFrame sdf = jarvey.read().dataset("경제/지오비전/유동인구/2015/시간대");
//		SpatialDataFrame sdf = jarvey.read().dataset("경제/지오비전/유동인구/2015/요일별");
//		SpatialDataFrame sdf = jarvey.read().dataset("경제/지오비전/유동인구/2015/성연령");
//		SpatialDataFrame sdf = jarvey.read().dataset("경제/지오비전/카드매출/2015/시간대");
		sdf = jarvey.read().dataset(dsId);
		sdf.printSchema();
		sdf.show(10);
		
//		String str = FStream.from(sdf.getJarveySchema().getColumnAll())
//							.map(jc -> jc.getName().get())
//							.join(",");
//		System.out.println(str);
//		
//		System.out.println(sdf.count());
		
		jarvey.spark().stop();
	}
}
