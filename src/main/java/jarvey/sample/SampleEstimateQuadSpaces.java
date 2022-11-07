package jarvey.sample;

import java.util.TreeSet;

import org.apache.hadoop.fs.Path;

import jarvey.JarveySession;
import jarvey.cluster.EstimateQuadSpaces;
import jarvey.cluster.EstimateQuadSpaces.PartitionEstimate;
import jarvey.cluster.EstimateQuadSpacesOptions;

import utils.func.Tuple;


public class SampleEstimateQuadSpaces {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("estimate_clusters")
											.master("local[10]")
											.hadoopDatasetRoot(new Path("jarvey-hadoop.xml"), "jarvey")
											.getOrCreate();

//		String dsId = "구역/읍면동";
//		String dsId = "구역/집계구";
//		String dsId = "교통/나비콜";
//		String dsId = "행자부/도로명주소/건물_위치정보";
//		String dsId = "건물/GIS건물통합정보_2019";
		String dsId = "경제/지오비전/집계구/2015";
//		String dsId = "구역/연속지적도";
		EstimateQuadSpacesOptions opts = EstimateQuadSpacesOptions.create().sampleRatio(0.3)
//																	.clusterSizeHint("64mb")
//																	.clusterSizeLimit("96mb")
																	.clusterSizeHint("32mb")
																	.clusterSizeLimit("40mb")
																	.save(true);
		
		EstimateQuadSpaces estimate = new EstimateQuadSpaces(jarvey, dsId, opts);
		Tuple<Integer, TreeSet<PartitionEstimate>> result = estimate.call();
		int nsamples = result._1;
		TreeSet<PartitionEstimate> clusters = result._2;
		
		System.out.printf("# of samples: %d%n", nsamples);
		System.out.printf("# of quad-spaces: %d%n", clusters.size());
		
		for ( EstimateQuadSpaces.PartitionEstimate qspace: clusters ) {
			System.out.printf("  %s%n", qspace);
		}
		
		jarvey.spark().stop();
	}
}
