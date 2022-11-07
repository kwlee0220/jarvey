package jarvey.sample;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;

import java.io.File;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.optor.geom.RangePredicate;
import jarvey.optor.geom.join.SpatialDataFrameSummary;
import jarvey.optor.geom.join.SpatialJoinOptions;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.JarveySchema;

import utils.StopWatch;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class EvaluateFilterRange {
	private static final String SGG = "구역/시군구_2019";
	private static final String SAFTY_ZONES = "POI/어린이보호구역";
	private static final String DTG_2016 = "교통/DTG/2016";
	private static final String BUILDINGS = "건물/GIS건물통합정보_2019";
	
	private static final double DISTANCE = 500.0;
	
	public static final void main(String... args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("range_query")
											.master("local[7]")
											.getOrCreate();

		SpatialDataFrame buildings = jarvey.read().dataset(BUILDINGS);
		GeometryColumnInfo gcInfo = buildings.assertDefaultGeometryColumnInfo();
		
		SpatialDataFrameSummary summary = jarvey.read().dataset(SGG)
												.transformCrs(gcInfo.getSrid())
												.summarizeSpatialInfo();
		
		SpatialDataFrame saftyZones = jarvey.read().dataset(SAFTY_ZONES)
													.assignUid("uid")
													.transformCrs(gcInfo.getSrid())
													.filter(RangePredicate.isCoveredBy(summary.getBounds()));
		SpatialDataFrame inner = saftyZones.select("the_geom", "uid", "대상시설명");
		Broadcast<List<Row>> bvar = jarvey.javaSc().broadcast(inner.collectAsList());
		
		countNearBuildings(jarvey, inner.getJarveySchema(), bvar);
		averageSpeed(jarvey, inner.getJarveySchema(), bvar, summary.getBounds());
		
//		process(jarvey, summary);
		
		jarvey.spark().stop();
	}
	
	private static void countNearBuildings(JarveySession jarvey, JarveySchema innerSchema,
														Broadcast<List<Row>> inner) {
		StopWatch watch = StopWatch.start();
		
		SpatialDataFrame buildings = jarvey.read().dataset(BUILDINGS);
		
		SpatialJoinOptions opts = SpatialJoinOptions.WITHIN_DISTANCE(DISTANCE)
													.joinType("innerJoin")
													.outputColumns("right.*-{the_geom}");
		SpatialDataFrame result = buildings.spatialBroadcastJoin(innerSchema, inner, opts);
		Dataset<Row> counts = result.groupBy("uid").count();
		jarvey.toSpatial(counts)
				.writeSpatial()
				.force(true)
				.dataset("tmp/result1");
		
		System.out.printf("near buildings: elapsed: %s%n", watch.stopAndGetElpasedTimeString());
	}
	
	private static void averageSpeed(JarveySession jarvey, JarveySchema innerSchema, Broadcast<List<Row>> inner,
										Envelope coverage) {
		StopWatch watch = StopWatch.start();
		
		SpatialJoinOptions opts = SpatialJoinOptions.WITHIN_DISTANCE(DISTANCE)
													.joinType("innerJoin")
													.outputColumns("left.운행속도,right.*-{the_geom}");
		
		SpatialDataFrame dtg = jarvey.read().dataset(DTG_2016);
		
		Dataset<Row> avgSpeeds = dtg.filter("`운행속도` > 1")
									.filter(RangePredicate.isCoveredBy(coverage))
									.transformCrs(5174)
									.spatialBroadcastJoin(innerSchema, inner, opts)
									.groupBy("uid").agg(count("운행속도").as("count"), avg("운행속도").as("speed"))
									.filter("count > 10000");
		jarvey.toSpatial(avgSpeeds)
				.writeSpatial()
				.force(true)
				.dataset("tmp/result2");
		
		System.out.printf("near traffic: elapsed: %s%n", watch.stopAndGetElpasedTimeString());
	}
	
}
