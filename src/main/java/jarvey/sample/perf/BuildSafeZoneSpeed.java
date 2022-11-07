package jarvey.sample.perf;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import java.io.File;
import java.util.concurrent.Callable;

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

import utils.StopWatch;
import utils.geo.util.CoordinateTransform;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class BuildSafeZoneSpeed implements Callable<Void> {
	private static final String SIDO = "구역/시도";
	private static final String SAFTY_ZONES = "POI/어린이보호구역";
//	private static final String DTG_2018 = "교통/DTG/2018";
	private static final String DTG_2018 = "교통/DTG/test";
	private static final String OUTPUT = "tmp/safe_zone_speed";

	private static final int DEFAULT_SRID = 5186;
	private static final double DISTANCE = 300.0;
	private static final int MIN_SAMPLE_COUNT = 10;
	
	private final JarveySession m_jarvey;
	private final String m_dsId;
	private final String m_outDsId;
	private final double m_distance;
	private final int m_minSamples;
	
	public BuildSafeZoneSpeed(JarveySession jarvey, String dsId, String outDsId,
							double distance, int minSampleCount) {
		m_jarvey = jarvey;
		m_dsId = dsId;
		m_outDsId = outDsId;
		m_distance = distance;
		m_minSamples = minSampleCount;
	}

	@Override
	public Void call() throws Exception {
		Envelope bounds = getBounds(SIDO, 4326, m_distance + 1);
		
		SpatialDataFrame saftyZones = m_jarvey.read().dataset(SAFTY_ZONES)
											.withRegularColumn("uid", monotonically_increasing_id())
											.persist();
		GeometryColumnInfo gcInfo = saftyZones.assertDefaultGeometryColumnInfo();
		
		SpatialDataFrame validZones = saftyZones.select("the_geom", "uid")
												.filter(RangePredicate.isCoveredBy(bounds))
												.transformCrs(DEFAULT_SRID);
		
		SpatialJoinOptions opts = SpatialJoinOptions.WITHIN_DISTANCE(m_distance)
													.outputColumns("right.uid,left.speed");
		SpatialDataFrame tmp = m_jarvey.read().dataset(m_dsId)
										.filter("speed > 1")
										.filter(RangePredicate.isCoveredBy(bounds))
										.transformCrs(DEFAULT_SRID)
										.spatialBroadcastJoin(validZones, opts);
		Dataset<Row> speeds = tmp.toDataFrame()
								.groupBy("uid").agg(count("speed").as("count"), avg("speed").as("avg_speed"))
								.filter("count > " + m_minSamples)
								.select("uid", "avg_speed");
		
		Dataset<Row> resultDf = saftyZones.toDataFrame()
											.select("uid", "the_geom", "대상시설명", "소재지도로명주소")
											.join(speeds, "uid")
											.drop("uid")
											.orderBy(desc("avg_speed"));
		SpatialDataFrame result = m_jarvey.toSpatial(resultDf, gcInfo);
		result.writeSpatial().force(true).dataset(m_outDsId);
		
		return null;
	}
	
	private Envelope getBounds(String dsId, int targetSrid, double margin) {
		SpatialDataFrame sdf = m_jarvey.read().dataset(dsId);
		
		SpatialDataFrameSummary summary = sdf.summarizeSpatialInfo();
		Envelope bounds = new Envelope(summary.getBounds());
		
		int expandSrid = (targetSrid != 4326) ? targetSrid : DEFAULT_SRID;
		if ( sdf.getSrid() != expandSrid ) {
			bounds = CoordinateTransform.transform(bounds, sdf.getSrid(), expandSrid);
		}
		bounds.expandBy(margin);
		if ( expandSrid != targetSrid ) {
			bounds = CoordinateTransform.transform(bounds, expandSrid, targetSrid);
		}
		
		return bounds;
	}
	
	public static final void main(String... args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("range_query")
											.master("local[7]")
											.getOrCreate();

		StopWatch watch = StopWatch.start();
		
		BuildSafeZoneSpeed op = new BuildSafeZoneSpeed(jarvey, DTG_2018, OUTPUT, DISTANCE, MIN_SAMPLE_COUNT);
		op.call();
		watch.stop();
		
		SpatialDataFrame result = jarvey.read().dataset(OUTPUT);
		long count = result.count();
		System.out.printf("done: count=%d, elapsed=%s%n", count, watch.getElapsedMillisString());

		System.in.read();
		jarvey.spark().stop();
	}
}
