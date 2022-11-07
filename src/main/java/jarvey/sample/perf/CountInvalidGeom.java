package jarvey.sample.perf;

import static jarvey.optor.geom.GeometryPredicate.not;
import static jarvey.optor.geom.RangePredicate.isCoveredBy;

import java.io.File;
import java.util.concurrent.Callable;

import org.locationtech.jts.geom.Envelope;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.optor.geom.join.SpatialDataFrameSummary;

import utils.StopWatch;
import utils.geo.util.CoordinateTransform;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class CountInvalidGeom implements Callable<Long> {
//	private static final String INPUT = "교통/DTG/2018";
	private static final String INPUT = "교통/DTG/2016";
//	private static final String INPUT = "교통/나비콜";
//	private static final String INPUT = "POI/어린이보호구역";
	private static final String BOUND = "구역/시군구_2019";
	private static final int DEFAULT_SRID = 5186;
	
	private final JarveySession m_jarvey;
	private final String m_dsId;
	private final String m_boundDsId;
	
	public CountInvalidGeom(JarveySession jarvey, String dsId, String boundDsId) {
		m_jarvey = jarvey;
		m_dsId = dsId;
		m_boundDsId = boundDsId;
	}
	
	public static final void main(String... args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("count_invalid_geom")
											.master("local[7]")
											.getOrCreate();
		StopWatch watch = StopWatch.start();

		CountInvalidGeom job = new CountInvalidGeom(jarvey, INPUT, BOUND);
		long invalidCount = job.call();
		System.out.printf("done: invalid count=%d, elapsed=%s%n",
						invalidCount, watch.getElapsedMillisString());

		System.in.read();
		jarvey.spark().stop();
	}
		
	@Override
	public Long call() throws Exception {
		SpatialDataFrame sdf = m_jarvey.read().dataset(m_dsId);
		
		Envelope bounds = getBounds(m_boundDsId, sdf.getSrid(), 1);
		return m_jarvey.read().dataset(m_dsId)
						.filter(not(isCoveredBy(bounds)))
						.count();
	}
	
	private Envelope getBounds(String dsId, int targetSrid, int margin) {
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
}
