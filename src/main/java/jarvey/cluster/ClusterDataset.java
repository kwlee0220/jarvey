package jarvey.cluster;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import jarvey.JarveySession;
import jarvey.support.MapTile;
import utils.func.KeyValue;
import utils.func.Tuple;
import utils.stream.FStream;

/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class ClusterDataset implements Callable<Void> {
	private static final Logger s_logger = LoggerFactory.getLogger(ClusterDataset.class);
	private static final double OUTLIER_THRESHOLD = 0.1f;
	private static final double MAX_OUTLIER_FILL_RATIO = 0.7f;
	
	private final JarveySession m_jarvey;
	private final String m_dsId;
	private final String m_outDsId;
	private final ClusterDatasetOptions m_opts;
	
	public ClusterDataset(JarveySession jarvey, String dsId, String outDsId, ClusterDatasetOptions opts) {
		m_jarvey = jarvey;
		m_dsId = dsId;
		m_outDsId = outDsId;
		m_opts = opts;
	}
	
	public ClusterDataset(JarveySession jarvey, String dsId, ClusterDatasetOptions opts) {
		this(jarvey, dsId, dsId + "_clustered", opts);
	}

	@Override
	public Void call() throws IOException {
		Long[] qids = m_opts.candidateQuadIds();
		if ( qids == null || qids.length == 0 ) {
			EstimateQuadKeysOptions estOpts = m_opts.toEstimateQuadKeysOptions();
			EstimateQuadKeys estimate = new EstimateQuadKeys(m_jarvey, m_dsId, estOpts);
			Map<String,Tuple<Integer,Float>> clustHist = estimate.call();
				
			List<KeyValue<String,Tuple<Integer,Float>>> outlierCandidates
												= FStream.from(clustHist)
														.filterValue(t -> t._2 < OUTLIER_THRESHOLD)
														.sortByValue((t1,t2) -> Float.compare(t1._2, t2._2))
														.toList();
			float fillRatio = 0f;
			List<KeyValue<String,Tuple<Integer,Float>>> ignoredKvs = Lists.newArrayList();
			for ( KeyValue<String,Tuple<Integer,Float>> kv: outlierCandidates ) {
				if ( (fillRatio += kv.value()._2) > MAX_OUTLIER_FILL_RATIO ) {
					break;
				}
				ignoredKvs.add(kv);
			}
			
			Set<String> ignoredQids = FStream.from(ignoredKvs)
												.map(kv -> kv.key())
												.toCollection(Sets.newLinkedHashSet());
			qids = FStream.from(clustHist)
							.toKeyStream()
							.filter(qid -> !ignoredQids.contains(qid))
							.map(MapTile::toQuadId)
							.toArray(Long.class);
			if ( s_logger.isInfoEnabled() ) {
				int nignoreds = clustHist.size() - qids.length;
				String ignoreds = FStream.from(ignoredQids)
										.map(qid -> KeyValue.of(qid,  clustHist.get(qid)))
										.map(kv -> String.format("%s(%.1f%%)", kv.key(), kv.value()._2*100))
										.join(", ");
				s_logger.info("Total {} ({} - {}) clusters", qids.length, clustHist.size(), nignoreds);
				String msg = String.format("Dropped tiny %d clusters (fill_ratio=%.2f): %s",
											nignoreds, fillRatio, ignoreds);
				s_logger.info(msg);
			}
		}
		
		m_jarvey.read()
				.dataset(m_dsId)
				.cluster(m_outDsId, qids, m_opts.force());
		
		return null;
	}
}
