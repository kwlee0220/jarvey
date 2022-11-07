package jarvey.optor.geom.join;

import org.apache.spark.sql.Row;

import jarvey.SpatialDataFrame;
import jarvey.support.colexpr.JoinColumnSelector;
import jarvey.type.JarveySchema;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CoGroupSpatialInnerJoin extends CoGroupSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private final JoinColumnSelector m_selector;

	public CoGroupSpatialInnerJoin(SpatialDataFrame left, SpatialDataFrame right, SpatialJoinOptions opts) {
		super(left, right, opts);

		m_selector = m_opts.outputColumns()
							.map(expr -> new JoinColumnSelector(left.getJarveySchema(),
																right.getJarveySchema(), expr))
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
	protected FStream<Row> handleNestedLoopMatches(QuadInfoRow outer,
															FStream<QuadInfoRow> inners) {
		return inners.map(inner -> m_selector.select(m_left.getJarveySchema(), outer.getRow(),
													m_right.getJarveySchema(), inner.getRow()));
	}
	
	@Override
	public String toString() {
		return String.format("%s", getClass().getSimpleName());
	}
}
