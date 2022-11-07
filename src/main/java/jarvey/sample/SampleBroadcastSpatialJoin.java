package jarvey.sample;

import static org.apache.spark.sql.functions.desc;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.optor.geom.join.SpatialJoinOptions;

import utils.StopWatch;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class SampleBroadcastSpatialJoin {
	public static final void main(String... args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("broadcast_spatial_join")
											.master("local[7]")
											.getOrCreate();
		
//		String leftDfId = "POI/주유소_가격";
		String leftDfId = "교통/나비콜/2016/01";
		String rightDfId = "구역/읍면동";
		
		StopWatch watch = StopWatch.start();

//		SpatialJoinOptions opts = SpatialJoinOptions.OUTPUT("left.*-{the_geom,주소,상표,셀프여부},right.*-{the_geom}");
		SpatialJoinOptions opts = SpatialJoinOptions.OUTPUT("left.carno,right.*-{the_geom}");
		SpatialDataFrame left = jarvey.read().dataset(leftDfId);
		SpatialDataFrame right = jarvey.read().dataset(rightDfId)
											.transformCrs(4326)
											.select("the_geom", "EMD_KOR_NM", "EMD_CD");

		Dataset<Row> joined = left.spatialBroadcastJoin(right, opts)
									.groupBy("EMD_CD", "EMD_KOR_NM").count()
									.sort(desc("count"))
									.coalesce(1);
		SpatialDataFrame result = jarvey.toSpatial(joined);
		result.writeSpatial().force(true).dataset("tmp/result");
		
		long count = jarvey.read().dataset("tmp/result").count();
		System.out.printf("count=%d, elapsed=%s%n", count, watch.stopAndGetElpasedTimeString());
		
		jarvey.spark().stop();
	}
}
