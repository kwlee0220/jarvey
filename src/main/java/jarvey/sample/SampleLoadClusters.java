package jarvey.sample;

import java.io.File;
import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.MinMaxPriorityQueue;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.support.RecordLite;
import jarvey.type.JarveySchema;

import utils.StopWatch;
import utils.UnitUtils;
import utils.geo.util.CoordinateTransform;
import utils.stream.FStream;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class SampleLoadClusters {
	public static final void main(String[] args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("range_query")
											.master("local[7]")
											.getOrCreate();
		
//		String dsId = "POI/주유소_가격";
//		String dsId = "POI/민원행정기관";
//		String dsId = "구역/읍면동";
//		String dsId = "구역/집계구";
//		String dsId = "건물/건물_위치정보";
//		String dsId = "교통/나비콜";
		String dsId = "구역/연속지적도_2019";

		String sggKey = "11650";		// 서울시 서초구
		String emdKey = "11110126";		// 서울특별시 종로구 종로1가
		String liKey = "4613034026";	// 여수시 남면 연도리
		
//		jarvey.read().clusters(dsId, new long[] {-9}).show(50);
		
		JarveySchema jschema = jarvey.read().readClusterJarveySchema(dsId);
		int srid = jschema.getSrid();

		Geometry keyGeom = fromSgg(jarvey, sggKey, srid);
//		Geometry keyGeom = fromEmd(jarvey, emdKey, srid);
//		Geometry keyGeom = fromLi(jarvey, liKey, srid);
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
		
		SpatialDataFrame sdf = jarvey.read().clusters(dsId, key);
		long cnt = sdf.count();
		watch.stop();
//		System.out.printf("count=%d, elapsed: %s%n", cnt, watch.getElapsedMillisString());
		
		return watch.getElapsedInMillis();
	}
	
	public static final Geometry fromSgg(JarveySession jarvey, String id, int srid) {
		String expr = String.format("A1 == '%s'", id);
		SpatialDataFrame sdf = jarvey.read().dataset("구역/시군구_2019")
									.filter(expr);
		if ( sdf.assertDefaultGeometryColumnInfo().getSrid() != srid ) {
			sdf = sdf.transformCrs(srid);
		}
		
		List<RecordLite> recList = sdf.select("the_geom", "A2").collectAsRecordList();
		if ( recList.size() == 0 ) {
			throw new IllegalArgumentException("invalid SGG key: " + id);
		}
		RecordLite rec = recList.get(0);
		System.out.println("key=" + rec.getString(1));
		return rec.getGeometry(0);
	}
	
	public static final Geometry fromEmd(JarveySession jarvey, String id, int srid) {
		String expr = String.format("A1 == '%s'", id);
		SpatialDataFrame sdf = jarvey.read().dataset("구역/읍면동_2019")
									.filter(expr);
		if ( sdf.assertDefaultGeometryColumnInfo().getSrid() != srid ) {
			sdf = sdf.transformCrs(srid);
		}
		
		List<RecordLite> recList = sdf.select("the_geom", "A2").collectAsRecordList();
		if ( recList.size() == 0 ) {
			throw new IllegalArgumentException("invalid EMD key: " + id);
		}
		RecordLite rec = recList.get(0);
		System.out.println("key=" + rec.getString(1));
		return rec.getGeometry(0);
	}
	
	public static final Geometry fromLi(JarveySession jarvey, String id, int srid) {
		String expr = String.format("A1 == '%s'", id);
		SpatialDataFrame sdf = jarvey.read().dataset("구역/리_2019")
									.filter(expr);
		if ( sdf.assertDefaultGeometryColumnInfo().getSrid() != srid ) {
			sdf = sdf.transformCrs(srid);
		}
		
		List<RecordLite> recList = sdf.select("the_geom", "A2").collectAsRecordList();
		if ( recList.size() == 0 ) {
			throw new IllegalArgumentException("invalid LI key: " + id);
		}
		RecordLite rec = recList.get(0);
		System.out.println("key=" + rec.getString(1));
		return rec.getGeometry(0);
	}
}
