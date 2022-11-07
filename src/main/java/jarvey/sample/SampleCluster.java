package jarvey.sample;

import jarvey.JarveySession;
import jarvey.cluster.ClusterDataset;
import jarvey.cluster.ClusterDatasetOptions;
import jarvey.cluster.SpatialClusterFile;

import utils.StopWatch;

public class SampleCluster {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[7]")
											.getOrCreate();

//		String dsId = "POI/민원행정기관";
//		String dsId = "구역/읍면동";
//		String dsId = "구역/집계구";
//		String dsId = "건물/건물_위치정보";
		String dsId = "교통/나비콜";
//		String dsId = "건물/GIS건물통합정보_2019";
//		String dsId = "구역/연속지적도";

		ClusterDatasetOptions opts = ClusterDatasetOptions.FORCE
														.clusterSizeHint("64mb")
														.clusterSizeLimit("80mb")
														.outlierSizeLimit("1mb")
														.dropFinalOutliers(true)
														.sampleRatio(0.3);
//		ClusterDatasetOptions opts = ClusterDatasetOptions.FORCE
//														.clusterSizeHint("16mb")
//														.clusterSizeLimit("20mb")
//														.outlierSizeLimit("1mb")
//														.dropFinalOutliers(true)
//														.sampleRatio(0.5);
		ClusterDataset cluster = new ClusterDataset(jarvey, dsId, opts);
		
		StopWatch watch = StopWatch.start();
		SpatialClusterFile scFile = cluster.call();
		watch.stop();
		
		scFile.streamPartitions(false).forEach(System.out::println);
		
		System.out.println("nclusters=" + scFile.toQuadIds().length
							+", elapsed: " + watch.getElapsedSecondString());
		
		jarvey.spark().stop();
	}
}
