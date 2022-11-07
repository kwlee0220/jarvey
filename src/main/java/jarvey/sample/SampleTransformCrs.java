package jarvey.sample;

import java.io.File;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class SampleTransformCrs {
	public static final void main(String[] args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("range_query")
											.master("local[10]")
											.getOrCreate();
		
		String keyDsId = "구역/읍면동";
		
		SpatialDataFrame keySds = jarvey.read().dataset(keyDsId)
										.transformCrs(4326)
										.select("the_geom")
										.limit(5);
		
		keySds.collectAsRecordList().forEach(System.out::println);
		
		jarvey.spark().stop();
	}
}
