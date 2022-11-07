package jarvey.test;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.type.JarveySchema;

import utils.StopWatch;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class TestDifferenceJoin {
	private static final String ID = SpatialDataFrame.COLUMN_PARTITION_QID;
	private static final String MEMBERS = SpatialDataFrame.COLUMN_QUAD_IDS;
	private static final String ENVL = SpatialDataFrame.COLUMN_ENVL4326;
	
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[10]")
											.getOrCreate();
		
		String baseId = "poi_buildings";
		String baseClusterId = baseId + "_clustered";

		String leftId = "district_output_kostat";
		String rightId = "poi_buildings";
		String leftClusterId = leftId + "_clustered";
		String rightClusterId = rightId + "_clustered";

		JarveySchema jschema = jarvey.loadJarveySchema(baseClusterId);
		long[] quadIds = jschema.getQuadIds();
		int srid = jschema.getDefaultGeometryColumnInfo().getSrid();
		
		StopWatch watch = StopWatch.start();

		String outCols = "left.the_geom,left.ADM_CD as code,right.법정동코드";
		SpatialDataFrame left = jarvey.read().dataset(leftClusterId).transformCrs(srid);
		SpatialDataFrame right = jarvey.read().dataset(rightClusterId).transformCrs(srid);
//		SpatialDataFrame output = left.differenceJoin(right);
//		
//		output.show(5);
//		
//		long count1 = output.count();
//		System.out.println(count1);
//		
//		System.out.println("elapsed: " + watch.getElapsedSecondString());
		
		jarvey.spark().stop();
	}
}
