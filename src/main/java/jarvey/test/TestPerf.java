package jarvey.test;

import static jarvey.jarvey_functions.ST_Area;
import static jarvey.jarvey_functions.ST_Intersection;
import static jarvey.jarvey_functions.ST_Intersects;
import static jarvey.jarvey_functions.ST_ReducePrecision;
import static jarvey.jarvey_functions.TEST_Intersects;
import static jarvey.optor.geom.JarveySpatialFunctions.intersectRange;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.MinMaxPriorityQueue;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.sample.SampleLoadClusters;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

public class TestPerf {
	public static final void main(String... args) throws Exception {
		String dsId = "구역/연속지적도_2019";

		String sggKey = "11650";		// 서울시 서초구
		String emdKey = "11110126";		// 서울특별시 종로구 종로1가
		String liKey = "4613034026";	// 여수시 남면 연도리
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[7]")
											.getOrCreate();
		
		JarveySchema jschema = jarvey.read().readClusterJarveySchema(dsId);
		int srid = jschema.getSrid();
		
		evaluateEmd(jarvey, dsId, srid, 1);
		evaluateSgg(jarvey, dsId, srid, 1);
		
		System.out.println();
		System.out.println("----------------------------------------------------------------------------------");
		System.out.println();
		
		evaluateEmd(jarvey, dsId, srid, 10);
		evaluateSgg(jarvey, dsId, srid, 10);
		
		jarvey.spark().stop();
	}
	
	private static void evaluateEmd(JarveySession jarvey, String dsId, int srid, int niter) throws Exception {
		String keyCode = "11110126";		// 서울특별시 종로구 종로1가
		Geometry key = SampleLoadClusters.fromEmd(jarvey, keyCode, srid);
		
		Evaluation testCase;
		long millis;
		
		testCase = new GeomApiCluster(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new GeomApiDataFrame(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new UDFCluster(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new UDFDataFrame(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new SingleUDFDataFrame(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new GeomApiSimple(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new GeomApiSimple2(jarvey, dsId, key, false);
		millis = evaluate(testCase, 10);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new UDFSimple(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new UDFSimple2(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
	}
	
	private static void evaluateSgg(JarveySession jarvey, String dsId, int srid, int niter) throws Exception {
		String keyCode = "11650";		// 서울시 서초구
		Geometry key = SampleLoadClusters.fromSgg(jarvey, keyCode, srid);
		
		Evaluation testCase;
		long millis;
		
		testCase = new GeomApiCluster(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new GeomApiDataFrame(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new UDFCluster(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new UDFDataFrame(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new SingleUDFDataFrame(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new GeomApiSimple(jarvey, dsId, key, false);
		millis = evaluate(testCase, 10);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new GeomApiSimple2(jarvey, dsId, key, false);
		millis = evaluate(testCase, 10);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new UDFSimple(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
		
		testCase = new UDFSimple2(jarvey, dsId, key, false);
		millis = evaluate(testCase, niter);
		System.out.printf("%s: key=%s, elapsed=%dms (%s)%n", testCase.getClass().getSimpleName(),
							keyCode, millis, UnitUtils.toMillisString(millis));
	}
	
	private static long evaluate(Evaluation testCase, int niter) throws Exception {
		MinMaxPriorityQueue<Long> minmax = MinMaxPriorityQueue.create();
		for ( int i=0; i < niter; ++i ) {
			minmax.add(testCase.run());
		}
		if ( niter > 2 ) {
			minmax.pollFirst();
			minmax.pollLast();
		}
		return FStream.from(minmax.stream()).mapToLong(v -> v).average().map(Math::round).get();
	}
	
	private static abstract class Evaluation {
		protected JarveySession m_jarvey;
		protected String m_dsId;
		protected Geometry m_key;
		protected boolean m_verbose;
		
		abstract protected long process();
		
		protected Evaluation(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			m_jarvey = jarvey;
			m_dsId = dsId;
			m_key = key;
			m_verbose = verbose;
		}

		public long run() throws Exception {
			StopWatch watch = StopWatch.start();
			long count = process();
			watch.stop();
			
			if ( m_verbose ) {
				System.out.printf("total: %d, elapsed=%s (%d)%n",
						count, watch.getElapsedMillisString(), watch.getElapsedInMillis());
			}
			return watch.getElapsedInMillis();
		}
	};
	
	private static class GeomApiCluster extends Evaluation {
		protected GeomApiCluster(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			super(jarvey, dsId, key, verbose);
		}
		
		@Override
		protected long process() {
			Envelope range = m_key.getEnvelopeInternal();
			
			SpatialDataFrame sdf = m_jarvey.read()
										.clusters(m_dsId, range)
										.filter(intersectRange(m_key))
										.reducePrecision(2)
										.intersection_with(m_key)
										.area("area")
										.filter("area > 5");
			return sdf.count();
		}
	};
	
	private static class GeomApiDataFrame extends Evaluation {
		protected GeomApiDataFrame(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			super(jarvey, dsId, key, verbose);
		}
		
		@Override
		protected long process() {
			Envelope range = m_key.getEnvelopeInternal();
			SpatialDataFrame sdf = m_jarvey.read()
										.dataset(m_dsId)
										.filter(intersectRange(m_key))
										.reducePrecision(2)
										.intersection_with(m_key)
										.area("area")
										.filter("area > 5");
			return sdf.count();
		}
	};
	
	private static class UDFCluster extends Evaluation {
		protected UDFCluster(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			super(jarvey, dsId, key, verbose);
		}
		
		@Override
		protected long process() {
			Envelope range = m_key.getEnvelopeInternal();
			SpatialDataFrame sdf = m_jarvey.read()
										.clustersJustForTest(m_dsId, range);
			
			GeometryColumnInfo gcInfo = sdf.assertDefaultGeometryColumnInfo();
			String geomCol = gcInfo.getName();
			GeometryType geomType = gcInfo.getDataType();

			sdf = sdf.filter(ST_Intersects(sdf.col(geomCol), m_key));
			sdf = sdf.withColumn(geomCol, ST_ReducePrecision(sdf.col(geomCol), 2), geomType);
			sdf = sdf.withColumn(geomCol, ST_Intersection(sdf.col(geomCol), m_key), geomType);
			sdf = sdf.withRegularColumn("area", ST_Area(sdf.col(geomCol)));
			sdf = sdf.filter("area > 5");
			return sdf.count();
		}
	};
	
	private static class UDFDataFrame extends Evaluation {
		protected UDFDataFrame(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			super(jarvey, dsId, key, verbose);
		}
		
		@Override
		protected long process() {
			SpatialDataFrame sdf = m_jarvey.read().dataset(m_dsId);
			
			GeometryColumnInfo gcInfo = sdf.assertDefaultGeometryColumnInfo();
			String geomCol = gcInfo.getName();
			GeometryType geomType = gcInfo.getDataType();

			sdf = sdf.filter(ST_Intersects(sdf.col(geomCol), m_key));
			sdf = sdf.withColumn(geomCol, ST_ReducePrecision(sdf.col(geomCol), 2), geomType);
			sdf = sdf.withColumn(geomCol, ST_Intersection(sdf.col(geomCol), m_key), geomType);
			sdf = sdf.withRegularColumn("area", ST_Area(sdf.col(geomCol)));
			sdf = sdf.filter("area > 5");
			return sdf.count();
		}
	}
	
	private static class SingleUDFDataFrame extends Evaluation {
		protected SingleUDFDataFrame(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			super(jarvey, dsId, key, verbose);
		}
		
		@Override
		protected long process() {
			SpatialDataFrame sdf = m_jarvey.read().dataset(m_dsId);
			
			GeometryColumnInfo gcInfo = sdf.assertDefaultGeometryColumnInfo();
			String geomCol = gcInfo.getName();

			sdf = sdf.filter(TEST_Intersects(sdf.col(geomCol), m_key));
			return sdf.count();
		}
	}
	
	private static class GeomApiSimple extends Evaluation {
		protected GeomApiSimple(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			super(jarvey, dsId, key, verbose);
		}
		
		@Override
		protected long process() {
			SpatialDataFrame sdf = m_jarvey.read().dataset(m_dsId)
											.area("area")
											.filter("area < 100000");
			return sdf.count();
		}
	}
	
	private static class GeomApiSimple2 extends Evaluation {
		protected GeomApiSimple2(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			super(jarvey, dsId, key, verbose);
		}
		
		@Override
		protected long process() {
			SpatialDataFrame sdf = m_jarvey.read().dataset(m_dsId)
											.area("area")
											.project("area")
											.filter("area < 100000");
			return sdf.count();
		}
	}
	
	private static class UDFSimple extends Evaluation {
		protected UDFSimple(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			super(jarvey, dsId, key, verbose);
		}
		
		@Override
		protected long process() {
			SpatialDataFrame sdf = m_jarvey.read().dataset(m_dsId);
			GeometryColumnInfo gcInfo = sdf.assertDefaultGeometryColumnInfo();
			String geomCol = gcInfo.getName();
			
			sdf = sdf.withRegularColumn("area", ST_Area(sdf.col(geomCol)))
					.filter("area < 100000");
			return sdf.count();
		}
	}
	
	private static class UDFSimple2 extends Evaluation {
		protected UDFSimple2(JarveySession jarvey, String dsId, Geometry key, boolean verbose) {
			super(jarvey, dsId, key, verbose);
		}
		
		@Override
		protected long process() {
			SpatialDataFrame sdf = m_jarvey.read().dataset(m_dsId);
			GeometryColumnInfo gcInfo = sdf.assertDefaultGeometryColumnInfo();
			String geomCol = gcInfo.getName();
			
			sdf = sdf.withRegularColumn("area", ST_Area(sdf.col(geomCol)))
					.select("area")
					.filter("area < 100000");
			return sdf.count();
		}
	}
}
