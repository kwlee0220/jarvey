package jarvey.sample;

import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.util.List;

import org.locationtech.jts.geom.Geometry;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.support.RecordLite;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class SampleUnaryIntersection {
	public static final void main(String[] args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("range_query")
											.master("local[10]")
											.getOrCreate();
		
//		String dsId = "POI/주유소_가격";
//		String dsId = "POI/민원행정기관";
//		String dsId = "교통/나비콜";
//		String dsId = "구역/시군구_2019";
		String dsId = "구역/연속지적도_2019";
		String key = "서초동";
		
		SpatialDataFrame sdf = jarvey.read().dataset(dsId);
		int srid = sdf.assertDefaultGeometryColumnInfo().getSrid();
		
		Geometry param = fromEmd(jarvey, key, srid);
		sdf = sdf.intersection_with(param).area("area").where("area > 10");
		sdf.show(100);
		
		jarvey.spark().stop();
	}
	
	private static final Geometry fromEmd(JarveySession jarvey, String id, int srid) {
		String keyDsId = "구역/읍면동_2019";
		
		SpatialDataFrame keySdf = jarvey.read().dataset(keyDsId);
		if ( keySdf.assertDefaultGeometryColumnInfo().getSrid() != srid ) {
			keySdf = keySdf.transformCrs(srid);
		}
		List<RecordLite> geoms = keySdf.filter(col("A2").equalTo(id))
										.transformCrs(srid)
										.select("the_geom")
										.collectAsRecordList();
		return geoms.get(0).getGeometry(0);
	}
}
