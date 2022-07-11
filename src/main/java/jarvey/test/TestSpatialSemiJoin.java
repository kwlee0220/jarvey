package jarvey.test;

import org.apache.log4j.PropertyConfigurator;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.type.JarveySchema;
import utils.StopWatch;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class TestSpatialSemiJoin {
	private static final String ID = SpatialDataset.CLUSTER_ID;
	private static final String MEMBERS = SpatialDataset.CLUSTER_MEMBERS;
	private static final String ENVL = SpatialDataset.ENVL4326;
	
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
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
		Long[] quadIds = jschema.getQuadIds();
		int srid = jschema.getDefaultGeometryColumnInfo().srid();
		
		StopWatch watch = StopWatch.start();

		String outCols = "left.the_geom,left.ADM_CD as code,right.법정동코드";
		SpatialDataset left = jarvey.read().dataset(leftClusterId).transform(srid);
		SpatialDataset right = jarvey.read().dataset(rightClusterId).transform(srid);
		SpatialDataset output = left.spatialSemiJoin(right);
		
		output.show(5);
		
		long count1 = output.count();
		System.out.println(count1);
		
		System.out.println("elapsed: " + watch.getElapsedSecondString());
		
		jarvey.spark().stop();
	}
}
