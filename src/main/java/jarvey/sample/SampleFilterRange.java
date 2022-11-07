package jarvey.sample;

import java.io.File;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.MinMaxPriorityQueue;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.optor.geom.RangePredicate;

import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class SampleFilterRange {
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
		String dsId = "구역/연속지적도_2019";

		String sggKey = "11650";		// 서울시 서초구
		String emdKey = "11110126";		// 서울특별시 종로구 종로1가
//		String liKey = "4613034026";	// 여수시 남면 연도리
		
		SpatialDataFrame sdf = jarvey.read().dataset(dsId);
		int srid = sdf.assertDefaultGeometryColumnInfo().getSrid();

		Geometry keyGeom = SampleLoadClusters.fromSgg(jarvey, sggKey, srid);
//		Geometry keyGeom = SampleLoadClusters.fromEmd(jarvey, emdKey, srid);
//		Geometry keyGeom = SampleLoadClusters.fromLi(jarvey, liKey, srid);
		Envelope keyEnvl = keyGeom.getEnvelopeInternal();
		
		MinMaxPriorityQueue<Long> minmax = MinMaxPriorityQueue.create();
		for ( int i =0; i < 10; ++i ) {
			minmax.add(process(jarvey, dsId, keyEnvl));
		}
		minmax.pollFirst();
		minmax.pollLast();
		double mean = FStream.from(minmax.stream()).mapToLong(v -> v).average().get();
		System.out.printf("mean elapsed: %s%n", UnitUtils.toMillisString((long)mean));
		
		jarvey.spark().stop();
	}
	
	private static long process(JarveySession jarvey, String dsId, Envelope key) {
		StopWatch watch = StopWatch.start();
		
		SpatialDataFrame sdf = jarvey.read().dataset(dsId).filter(RangePredicate.intersects(key));
		long cnt = sdf.count();
		watch.stop();
//		System.out.printf("count=%d, elapsed: %s%n", cnt, watch.getElapsedMillisString());
		
		return watch.getElapsedInMillis();
	}
	
}
