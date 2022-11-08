package jarvey.command;

import jarvey.JarveySession;
import jarvey.cluster.ClusterDataset;
import jarvey.cluster.ClusterDatasetOptions;
import jarvey.cluster.SpatialClusterFile;
import jarvey.cluster.SpatialPartitionFile;
import jarvey.type.JarveySchema;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;
import utils.io.FilePath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="marmot_spark_session",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="create Dataset")
public class ClusterDatasetMain extends JarveyCommand {
	@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
	private String m_dsId;

//	@Option(names= {"--create_index"}, description="create index file")
//	private boolean m_createIndex = false;

	@Option(names= {"--ref_dataset"}, paramLabel="dataset_id", description="reference dataset id")
	private void setReferenceDataset(String dsId) {
		m_refDsId = dsId;
	}
	private String m_refDsId = null;

	@Option(names= {"--sample_ratio"}, paramLabel="ratio", description="sampling ratio (default: 0.1)")
	private void setSampleRatio(double ratio) {
		m_sampleRatio = ratio;
	}
	private double m_sampleRatio = -1;

	@Option(names= {"--cluster_size"}, paramLabel="size", description="cluster size hint (default: '128mb')")
	private void setClusterSize(String sizeStr) {
		m_clusterSize = UnitUtils.parseByteSize(sizeStr);
	}
	private long m_clusterSize = -1;

	@Option(names={"-f", "--force"}, description="force to create clustered dataset")
	private boolean m_force;
	
	public static final void main(String... args) throws Exception {
		run(new ClusterDatasetMain(), args);
	}
	
	@Override
	protected void run(JarveySession jarvey) throws Exception {
		ClusterDatasetOptions opts = ClusterDatasetOptions.create().force(m_force);
		if ( m_refDsId != null ) {
			FilePath clusterPath = jarvey.getClusterFilePath(m_refDsId);
			long[] qids = SpatialClusterFile.of(jarvey, clusterPath)
											.streamPartitions(true)
											.mapToLong(SpatialPartitionFile::getQuadId)
											.toArray();
			opts = opts.candidateQuadIds(qids);
		}
		else {
			if ( m_sampleRatio > 0 ) {
				opts = opts.sampleRatio(m_sampleRatio);
			}
			if ( m_clusterSize > 0 ) {
				opts = opts.clusterSizeHint(m_clusterSize);
			}
			if ( m_refDsId != null ) {
				JarveySchema jschema = jarvey.loadJarveySchema(m_refDsId);
				opts = opts.candidateQuadIds(jschema.getQuadIds());
			}
		}
		
		ClusterDataset cluster = new ClusterDataset(jarvey, m_dsId, opts);
		cluster.call();
		
//		if ( m_createIndex ) {
//			List<Long> values = scFile.streamPartitions(true)
//											.map(sp -> sp.getQuadId()) 
//											.toList();
//			Dataset<Row> qidDf = jarvey.parallelize(values, Long.class);
//			
//			jarvey.getFilePath(outDsId)
//			
//			
//		JavaSparkContext jsc = new JavaSparkContext(jarvey.spark().sparkContext());
//		JavaRDD<Object[]> jrdd = jsc.parallelize(values);
//		
//		Dataset<Row> df = jarvey.spark().createDataFrame(jrdd, Long.class);
//		
//		JarveySchema.builder()
//					.addRegularColumn("quadid", DataTypes.LongType)
//					.addRegularColumn("quadkey", DataTypes.StringType)
//					.build();
//		
//		
//		jarvey.read().dataset(m_outDsId);
	}
}
