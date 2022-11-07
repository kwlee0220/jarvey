package jarvey.sample.dtg;

import java.io.File;

import org.locationtech.jts.geom.Point;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.optor.geom.JarveySpatialFunctions;
import jarvey.support.GeoUtils;

import utils.StopWatch;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class ExtractCrossRoads {
	private static final String DTG_2018 = "교통/DTG/2018";
//	private static final String DTG_2018 = "교통/DTG/test";
	
	private static final String OUTPUT = "tmp/cross_roads";
	
	private static final double DISTANCE = 60.0;
	
	public static final void main(String... args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("extract_cross_roads")
											.master("local[7]")
											.getOrCreate();

		StopWatch watch = StopWatch.start();
		
		Point center = GeoUtils.toPoint(14179725.409531374,4353822.907287661);
		
		SpatialDataFrame dtg = jarvey.read().dataset(DTG_2018)
									.transformCrs(3857)
									.filter(JarveySpatialFunctions.withinDistance(center, DISTANCE));
		
		dtg.writeSpatial().force(true).dataset(OUTPUT);
		watch.stop();
		
		long count = jarvey.read().dataset(OUTPUT).count();
		
		System.out.printf("done: count=%d, elapsed=%s%n", count, watch.getElapsedMillisString());
		
		jarvey.spark().stop();
	}
}
