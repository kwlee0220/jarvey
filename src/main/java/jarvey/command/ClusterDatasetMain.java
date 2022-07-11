package jarvey.command;

import jarvey.JarveySession;
import jarvey.cluster.ClusterDataset;
import jarvey.cluster.ClusterDatasetOptions;
import jarvey.type.JarveySchema;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.StopWatch;
import utils.UnitUtils;

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
	
	@Parameters(paramLabel="output_dataset", index="1", arity="0..1",
				description={"output dataset id"})
	private String m_outDsId = null;

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

	@Option(names= {"--cluster_size"}, paramLabel="size", description="cluster size (default: '128mb')")
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
		StopWatch watch = StopWatch.start();
		
		ClusterDatasetOptions opts = ClusterDatasetOptions.create()
														.force(m_force);
		if ( m_refDsId != null ) {
			JarveySchema jschema = jarvey.loadJarveySchema(m_refDsId);
			opts = opts.candidateQuadIds(jschema.getQuadIds());
		}
		if ( m_clusterSize > 0 ) {
			opts = opts.clusterSizeHint(m_clusterSize);
		}
		if ( m_sampleRatio > 0 ) {
			opts = opts.sampleRatio(m_sampleRatio);
		}
		String outDsId = (m_outDsId != null) ? m_outDsId : m_dsId + "_clustered";
		
		ClusterDataset cluster = new ClusterDataset(jarvey, m_dsId, outDsId, opts);
		cluster.call();
		
		System.out.printf("clustered: dataset=%s elapsed=%s%n",
							m_dsId, watch.getElapsedMillisString());
	}
}
