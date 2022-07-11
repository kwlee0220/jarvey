package jarvey.test;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.cluster.ClusterDatasetOptions;
import jarvey.support.HdfsPath;
import jarvey.support.MapTile;
import jarvey.type.JarveySchema;
import utils.stream.FStream;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class Test2 {
	private static final String CLUSTER_ID = SpatialDataset.CLUSTER_ID;
	public static final void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		ClusterDatasetOptions opts = ClusterDatasetOptions.create().clusterSizeHint("128mb").force(true);
		JarveySchema jschema = jarvey.loadJarveySchema("district_cadastral_2019_clustered");
		Long[] qids = jschema.getQuadIds();
		HdfsPath dsRoot = jarvey.getHdfsPath("district_cadastral_2019_clustered");
		
		List<Long> bigQids = FStream.of(qids).concatWith(MapTile.QID_OUTLIER)
									.filter(qid -> {
										try {
											HdfsPath clusterPath = dsRoot.child( String.format("%s=%d", CLUSTER_ID, qid));
											long totalLength = clusterPath.getTotalLength();
											return totalLength > opts.clusterSizeHint();
										}
										catch ( IOException e ) {
											return false;
										}
									})
									.toList();
		
		jarvey.spark().stop();
	}
}
