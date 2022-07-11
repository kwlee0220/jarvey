package jarvey.join;

import org.apache.spark.sql.Row;

import jarvey.SpatialDataset;
import jarvey.support.colexpr.JoinColumnSelector;
import jarvey.type.JarveySchema;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class CoGroupSemiSpatialJoin extends CoGroupSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private final JoinColumnSelector m_selector;

	public CoGroupSemiSpatialJoin(SpatialDataset left, SpatialDataset right, SpatialJoinOptions opts) {
		super(left, right, opts);
		
		String projExpr = String.format("left.*-{%s, %s, %s}", SpatialDataset.ENVL4326,
												SpatialDataset.CLUSTER_MEMBERS, SpatialDataset.CLUSTER_ID);
		m_selector = new JoinColumnSelector(left.getJarveySchema(), right.getJarveySchema(), projExpr);
	}
	
	@Override
	public JarveySchema getOutputJarveySchema() {
		return m_selector.getOutputJarveySchema();
	}

	@Override
	protected FStream<Row> handleMatches(QidAttachedRow left, FStream<QidAttachedRow> matches) {
		return matches.take(1).map(right -> m_selector.select(left, right));
	}
}
