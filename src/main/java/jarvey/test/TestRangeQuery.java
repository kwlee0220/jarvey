package jarvey.test;

import static org.apache.spark.sql.functions.col;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import utils.stream.FStream;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.type.GeometryValue;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class TestRangeQuery {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		String dsId = "district_output_kostat_clustered";

//		Envelope range = fromEmd(jarvey, "파장동");
		Envelope range = fromSgg(jarvey, "유성구");
//		Envelope range = fromSd(jarvey, "충청남도");
		
		SpatialDataset sds = SpatialDataset.rangeQuery(jarvey, dsId, range);
		sds.explain(true);
		sds.printSchema();
		sds.drop("the_geom").show(10);
		
		jarvey.spark().stop();
	}
	
	private static final Envelope fromSd(JarveySession jarvey, String id) {
		String keyDsId = "district_sd";
		
		SpatialDataset keySds = jarvey.read().dataset(keyDsId)
										.filter(col("CTP_KOR_NM").equalTo(id))
										.transform(5186)
										.select("the_geom");
		Geometry key = FStream.from(keySds.toLocalIterator())
								.map(r -> GeometryValue.deserialize(r.getAs(0)))
								.findFirst().get();
		return key.getEnvelopeInternal();
	}
	
	private static final Envelope fromSgg(JarveySession jarvey, String id) {
		String keyDsId = "district_sgg";
		
		SpatialDataset keySds = jarvey.read().dataset(keyDsId)
										.filter(col("SIG_KOR_NM").equalTo(id))
										.transform(5186)
										.select("the_geom");
		Geometry key = FStream.from(keySds.toLocalIterator())
								.map(r -> GeometryValue.deserialize(r.getAs(0)))
								.findFirst().get();
		return key.getEnvelopeInternal();
	}
	
	private static final Envelope fromEmd(JarveySession jarvey, String id) {
		String keyDsId = "district_emd";
		
		SpatialDataset keySds = jarvey.read().dataset(keyDsId)
										.filter(col("EMD_KOR_NM").equalTo(id))
										.transform(5186)
										.select("the_geom");
		Geometry key = FStream.from(keySds.toLocalIterator())
								.map(r -> GeometryValue.deserialize(r.getAs(0)))
								.findFirst().get();
		Envelope range = key.getEnvelopeInternal();
		return range;
	}
}
