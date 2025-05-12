package jarvey.sample;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.size;

import java.util.Arrays;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.cluster.EstimateQuadSpaces;
import jarvey.cluster.EstimateQuadSpaces.PartitionEstimate;
import jarvey.cluster.EstimateQuadSpacesOptions;

import utils.Tuple;
import utils.stream.FStream;


public class SampleAttachQuadInfo {
	private static final String QID_COL = SpatialDataFrame.COLUMN_QUAD_IDS;
	private static final String PART_COL = SpatialDataFrame.COLUMN_PARTITION_QID;
	
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("sample_attach_qidinfo")
											.master("local[7]")
											.hadoopDatasetRoot(new Path("jarvey-hadoop.xml"), "jarvey")
											.getOrCreate();
		
//		String dsId = "구역/읍면동_2019";
		String dsId = "구역/집계구";
//		String dsId = "건물/건물_위치정보";
//		String dsId = "건물/GIS건물통합정보_2019";
		EstimateQuadSpacesOptions opts = EstimateQuadSpacesOptions.create()
															.clusterSizeHint("32mb")
															.clusterSizeLimit("48mb")
															.outlierSizeLimit("1mb")
															.sampleRatio(0.5);
		
		EstimateQuadSpaces estimate = new EstimateQuadSpaces(jarvey, dsId, opts);
		Tuple<Integer, TreeSet<PartitionEstimate>> result = estimate.call();
		int nsamples = result._1;
		long[] qids = FStream.from(result._2).mapToLong(c -> c.quadId()).toArray();
		System.out.printf("sample count=%d, qids=%s%n", nsamples, Arrays.toString(qids));
		
		SpatialDataFrame attacheds = jarvey.read().dataset(dsId)
											.attachQuadInfo(qids);
		attacheds.printSchema();
		attacheds.drop("the_geom")
				.filter(size(col(QID_COL)).gt(1))
				.withRegularColumn(PART_COL, explode(attacheds.col(QID_COL)))
//				.select("A2", SpatialDataFrame.COLUMN_PARTITION_ID, SpatialDataFrame.COLUMN_QUAD_IDS)
				.show(10, false);
		
		jarvey.spark().stop();
	}
}
