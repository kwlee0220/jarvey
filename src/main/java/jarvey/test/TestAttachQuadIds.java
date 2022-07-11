package jarvey.test;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;

import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.cluster.ClusterDatasetOptions;
import jarvey.cluster.EstimateQuadKeys;
import jarvey.support.HdfsPath;
import jarvey.support.MapTile;
import utils.func.Tuple;
import utils.stream.FStream;


public class TestAttachQuadIds {
	private static final String CLUSTER_ID_COL_NAME = SpatialDataset.CLUSTER_ID;
	private static final String CLUSTER_MEMBER_COL_NAME = SpatialDataset.CLUSTER_MEMBERS;
	
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		String dsId = "district_cadastral";
		ClusterDatasetOptions opts = ClusterDatasetOptions.FORCE
														.sampleRatio(0.01);
		
		EstimateQuadKeys estimate = new EstimateQuadKeys(jarvey, dsId, opts.toEstimateQuadKeysOptions());
		Map<String,Tuple<Integer,Float>> clustHist = estimate.call();
		Long[] qids = FStream.from(clustHist)
								.toKeyStream()
								.map(MapTile::toQuadId)
								.toArray(Long.class);
		int nparts = (int)FStream.from(clustHist).filterValue(t -> t._2 >= 0.1).count();

		SpatialDataset attacheds = jarvey.read().dataset(dsId)
											.attachQuadIds(qids);
		attacheds.printSchema();
		attacheds.drop("the_geom")
				.filter(size(col(CLUSTER_MEMBER_COL_NAME)).gt(1))
				.show(10);
		
		System.out.printf("nparts=%d\n", nparts);
		
		jarvey.spark().stop();
	}
	
	private static final boolean isClusterFile(HdfsPath path) {
		return path.getName().startsWith(SpatialDataset.CLUSTER_ID + "=");
	}
}
