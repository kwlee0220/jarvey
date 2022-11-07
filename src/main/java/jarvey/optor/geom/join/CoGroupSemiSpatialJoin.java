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
public final class CoGroupSemiSpatialJoin extends CoGroupSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private final JoinColumnSelector m_selector;

	public CoGroupSemiSpatialJoin(SpatialDataFrame left, SpatialDataFrame right, SpatialJoinOptions opts) {
		super(left, right, opts);
		
		String projExpr = String.format("left.*-{%s, %s, %s}",
										SpatialDataFrame.COLUMN_ENVL4326,
										SpatialDataFrame.COLUMN_QUAD_IDS,
										SpatialDataFrame.COLUMN_PARTITION_QID);
		m_selector = new JoinColumnSelector(left.getJarveySchema(), right.getJarveySchema(), projExpr);
	}
	
	@Override
	public JarveySchema getOutputSchema() {
		return m_selector.getOutputJarveySchema();
	}

	@Override
	protected FStream<Row> handleNestedLoopMatches(QuadInfoRow outer, FStream<QuadInfoRow> inners) {
		return inners.exists() ? FStream.of(outer.getRow()) : FStream.empty();
	}
}
