package jarvey.join;

import java.util.Iterator;

import org.apache.spark.api.java.function.CoGroupFunction;
import org.apache.spark.sql.Row;

import jarvey.SpatialDataset;
import jarvey.type.JarveySchema;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class CoGroupSpatialJoin implements CoGroupFunction<Long,Row,Row,Row> {
	private static final long serialVersionUID = 1L;
	
	protected final JarveySchema m_left;
	protected final JarveySchema m_right;
	protected final SpatialJoinOptions m_opts;
	
	private transient SpatialJoinMatcher m_matcher;

	public abstract JarveySchema getOutputJarveySchema();
	protected abstract FStream<Row> handleMatches(QidAttachedRow left, FStream<QidAttachedRow> matches);
	
	protected CoGroupSpatialJoin(SpatialDataset left, SpatialDataset right, SpatialJoinOptions opts) {
		m_left = left.getJarveySchema();
		m_right = right.getJarveySchema();
		m_opts = opts;
	}
	
	protected void setupContext() {
		if ( m_matcher == null ) {
			SpatialRelation joinExpr = m_opts.joinExpr()
											.map(SpatialRelation::parse)
											.getOrElse(SpatialRelation.INTERSECTS);
			m_matcher = SpatialJoinMatchers.from(joinExpr);
			m_matcher.open(m_left, m_right);
		}
	}

	@Override
	public Iterator<Row> call(Long key, Iterator<Row> left, Iterator<Row> right) throws Exception {
		setupContext();
		
		FStream<QidAttachedRow> leftGroup = FStream.from(left)
													.map(r -> new QidAttachedRow(m_left, r));
		FStream<QidAttachedRow> rightGroup = FStream.from(right)
													.map(r -> new QidAttachedRow(m_right, r));
		
		QTreeLookupTable<QidAttachedRow> qtLut = new QTreeLookupTable<>(key, rightGroup);
		return leftGroup.flatMap(lrow -> handleMatches(lrow, m_matcher.match(lrow, qtLut)))
						.iterator();
	}
}