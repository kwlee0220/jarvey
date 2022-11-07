package jarvey.test;

import static org.apache.spark.sql.functions.col;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.optor.geom.RangePredicate;
import jarvey.type.GeometryBean;

import utils.StopWatch;
import utils.stream.FStream;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class TestRangeQuery {
	public static final void main(String[] args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("range_query")
											.hadoopDatasetRoot(new Path("jarvey-hadoop.xml"), "jarvey")
											.master("local[5]")
											.getOrCreate();
		
//		String dsId = "transport/dtg_m";
//		String dsId = "transport/DTG/2016/09";
		String dsId = args[0];

//		Envelope range = fromEmd(jarvey, "파장동");
		Envelope range = fromSgg(jarvey, "종로구");
//		Envelope range = fromSd(jarvey, "경기도");
		
		SpatialDataFrame sds = jarvey.read().option("header", "true")
											.csv(dsId)
											.point("X좌표", "Y좌표", 4326, "the_geom")
											.select("the_geom");
//		System.out.println(sds.getDefaultGeometryColumnInfo());
//		sds.show(5);
//		sds.explain();
		long totalCount = 7772087113L;
		
		StopWatch watch = StopWatch.start();
		SpatialDataFrame result = sds.filter(RangePredicate.intersects(range));
		long nfiltereds = result.count();
		watch.stop();
		
		double velo = totalCount / watch.getElapsedInFloatingSeconds();
		String msg = String.format("count=%d/%d (%.1f) elapsed=%s, velo=%.1f/s",
									nfiltereds, totalCount, (double)nfiltereds/totalCount,
									watch.getElapsedMillisString(), velo);
		System.out.println(msg);
		
//		sds.explain(true);
//		sds.printSchema();
//		sds.drop("the_geom").show(500);
		
		jarvey.spark().stop();
	}
	
	private static final Envelope fromSd(JarveySession jarvey, String id) {
		String keyDsId = "구역/시도";
		
		SpatialDataFrame keySds = jarvey.read().dataset(keyDsId)
										.filter(col("CTP_KOR_NM").equalTo(id))
										.transformCrs(4326)
										.select("the_geom");
		Geometry key = FStream.from(keySds.toLocalIterator())
								.map(r -> GeometryBean.deserialize(r.getAs(0)))
								.findFirst().get();
		return key.getEnvelopeInternal();
	}
	
	private static final Envelope fromSgg(JarveySession jarvey, String id) {
		String keyDsId = "구역/시군구";
		
		SpatialDataFrame keySds = jarvey.read().dataset(keyDsId)
										.filter(col("SIG_KOR_NM").equalTo(id))
										.transformCrs(4326)
										.select("the_geom");
		Geometry key = FStream.from(keySds.toLocalIterator())
								.map(r -> GeometryBean.deserialize(r.getAs(0)))
								.findFirst().get();
		return key.getEnvelopeInternal();
	}
	
	private static final Envelope fromEmd(JarveySession jarvey, String id) {
		String keyDsId = "구역/읍면동";
		
		SpatialDataFrame keySds = jarvey.read().dataset(keyDsId)
										.filter(col("EMD_KOR_NM").equalTo(id))
										.transformCrs(4326)
										.select("the_geom");
		Geometry key = FStream.from(keySds.toLocalIterator())
								.map(r -> GeometryBean.deserialize(r.getAs(0)))
								.findFirst().get();
		Envelope range = key.getEnvelopeInternal();
		return range;
	}
}
