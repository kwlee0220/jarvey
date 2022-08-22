package jarvey.test;

import java.io.IOException;

import utils.StopWatch;
import utils.UnitUtils;
import utils.func.Tuple;
import utils.func.UncheckedPredicate;
import utils.stream.FStream;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.cluster.ClusterDataset;
import jarvey.cluster.ClusterDatasetOptions;
import jarvey.support.HdfsPath;
import jarvey.type.DataUtils;
import jarvey.type.JarveySchema;

public class TestCluster {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		String dsId = "district_cadastral";
		
		ClusterDatasetOptions opts = ClusterDatasetOptions.FORCE.sampleRatio(0.15);
		ClusterDataset cluster = new ClusterDataset(jarvey, dsId, opts);
		
		StopWatch watch = StopWatch.start();
		cluster.call();
		watch.stop();
		
		String clusterDsId = dsId + "_clustered";
		JarveySchema sdInfo = jarvey.loadJarveySchema(clusterDsId);
		
		
		
		HdfsPath dsPath = jarvey.getHdfsPath(dsId + "_clustered");
		dsPath.streamChildFiles()
				.filter(UncheckedPredicate.sneakyThrow(TestCluster::isClusterFile))
				.map(path -> {
					try {
						long size = FStream.from(path.walkRegularFileTree())
												.mapOrThrow(HdfsPath::getLength)
												.mapToLong(DataUtils::asLong)
												.sum();
						return Tuple.of(path, size);
					}
					catch ( IOException e ) {
						e.printStackTrace();
						return null;
					}
				})
				.sort((t1, t2) -> Long.compare(t2._2, t1._2))
				.forEach(t -> {
					String szStr = UnitUtils.toByteSizeString(t._2);
					System.out.printf("%s: %s\n", t._1.toString(), szStr);
				});
		System.out.println("nclusters=" + sdInfo.getQuadIds().length
							+", elapsed: " + watch.getElapsedSecondString());
		
		jarvey.spark().stop();
	}
	
	private static final boolean isClusterFile(HdfsPath path) {
		return path.getName().startsWith(SpatialDataset.CLUSTER_ID + "=");
	}
}
