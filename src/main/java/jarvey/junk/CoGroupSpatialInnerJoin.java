package jarvey.junk;

import jarvey.optor.geom.join.QuadInfoRecord;
import jarvey.optor.geom.join.SpatialJoinOptions;
import jarvey.support.RecordLite;
import jarvey.support.colexpr.JoinColumnSelector;
import jarvey.type.JarveySchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CoGroupSpatialInnerJoin extends CoGroupSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private final JoinColumnSelector m_selector;
	
	public CoGroupSpatialInnerJoin(JarveySchema left, JarveySchema right, SpatialJoinOptions opts) {
		super(left, right, opts);

		m_selector = m_opts.outputColumns()
							.map(expr -> new JoinColumnSelector(m_leftSchema, m_rightSchema, expr))
							.getOrNull();
		if ( m_selector == null ) {
			throw new IllegalArgumentException("output columns not specified");
		}
	}

	@Override
	public JarveySchema getOutputSchema() {
		return m_selector.getOutputJarveySchema();
	}
	
	@Override
	protected RecordLite combine(QuadInfoRecord left, QuadInfoRecord right) {
		return m_selector.select(m_leftSchema, left.getRecord(), m_rightSchema, right.getRecord());
	}
}
