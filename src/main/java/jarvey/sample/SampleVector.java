package jarvey.sample;

import java.io.File;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;

import utils.stream.FStream;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class SampleVector {
	public static final void main(String[] args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("range_query")
											.master("local[7]")
											.getOrCreate();
		
//		String dsId = "POI/주유소_가격";
//		String dsId = "POI/민원행정기관";
//		String dsId = "구역/읍면동";
//		String dsId = "교통/나비콜";
//		String dsId = "구역/연속지적도_2019";
		String dsId = "경제/지오비전/유동인구/2015/시간대";
		
		SpatialDataFrame sdf = jarvey.read().dataset(dsId);
		sdf.printSchema();
		sdf.show(10);
		
		String[] cols = FStream.range(0, 24).map(i -> String.format("AVG_%02dTMST", i)).toArray(String.class);
		
		SpatialDataFrame sdf2 = sdf.toVector(cols, "feature").drop(cols);
		sdf2.printSchema();
		sdf2.show(10, false);
		
		SpatialDataFrame sdf3 = sdf.toArray(cols, "array").drop(cols);
		sdf3.printSchema();
		sdf3.show(10, false);
		
		SpatialDataFrame sdf4 = sdf3.toVector("array", "feature");
		sdf4.printSchema();
		sdf4.show(10, false);
		
		jarvey.spark().stop();
	}
}
