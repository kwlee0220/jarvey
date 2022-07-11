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
public class CoGroupSpatialBlockJoin extends CoGroupSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private final JoinColumnSelector m_selector;

	public CoGroupSpatialBlockJoin(SpatialDataset left, SpatialDataset right, SpatialJoinOptions opts) {
		super(left, right, opts);
		
		m_selector = opts.outputColumns()
						.map(expr -> new JoinColumnSelector(m_left, m_right, expr))
						.getOrNull();
		if ( m_selector == null ) {
			throw new IllegalArgumentException("output columns not specified");
		}
	}
	
	@Override
	public JarveySchema getOutputJarveySchema() {
		return m_selector.getOutputJarveySchema();
	}

	@Override
	protected FStream<Row> handleMatches(QidAttachedRow left, FStream<QidAttachedRow> matches) {
		return matches.map(right -> m_selector.select(left, right));
	}
}
