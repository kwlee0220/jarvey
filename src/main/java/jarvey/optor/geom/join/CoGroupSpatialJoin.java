package jarvey.optor.geom.join;

import java.util.Iterator;

import org.apache.spark.api.java.function.CoGroupFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.SpatialDataFrame;
import jarvey.optor.FlatMapIterator;
import jarvey.type.JarveySchema;

import utils.StopWatch;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class CoGroupSpatialJoin implements CoGroupFunction<Long,Row,Row,Row> {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(CoGroupSpatialJoin.class);
	
	protected final SpatialDataFrame m_left;
	protected final SpatialDataFrame m_right;
	protected final SpatialJoinOptions m_opts;

	public abstract JarveySchema getOutputSchema();
	protected abstract FStream<Row> handleNestedLoopMatches(QuadInfoRow outer, FStream<QuadInfoRow> inners);
	
	protected CoGroupSpatialJoin(SpatialDataFrame left, SpatialDataFrame right, SpatialJoinOptions opts) {
		m_left = left;
		m_right = right;
		m_opts = opts;
	}
	
	protected void setupContext() {
	}

	@Override
	public Iterator<Row> call(Long key, Iterator<Row> left, Iterator<Row> right) throws Exception {
		final StopWatch watch = StopWatch.start();
		
		final SpatialJoinMatcher<QuadInfoRow> matcher = SpatialJoinMatchers.from( m_opts.joinExpr());
		matcher.open(m_left.getJarveySchema(), m_right.getJarveySchema());
		
		setupContext();
		
		// right row-set을 이용하여 lookup-table을 생성
		QuadInfoRowDeserializer rightSerde = QuadInfoRowDeserializer.of(m_right.getJarveySchema());
		FStream<QuadInfoRow> rightRows = FStream.from(right).map(rightSerde::deserialize);
		final QTreeLookupTable<QuadInfoRow> qtLut = new QTreeLookupTable<>(key, rightRows);
		
		// left row-set에 포함된 각 레코드에 대해 join 시도.
		final QuadInfoRowDeserializer leftSerde = QuadInfoRowDeserializer.of(m_left.getJarveySchema());
		return new FlatMapIterator<Row, Row>(left) {
			@Override
			protected Iterator<Row> apply(Row row) {
				QuadInfoRow outer = leftSerde.deserialize(row);
				FStream<QuadInfoRow> innerMatches = matcher.match(outer, qtLut)
															.filter(inner -> QuadInfoRow.isNonReplica(outer, inner));
				return handleNestedLoopMatches(outer, innerMatches).iterator();
			}

			@Override
			protected void onIteratorFinished() {
				if ( s_logger.isInfoEnabled() ) {
					s_logger.info("done {}: partition={}, left={}, right={}, output={}, elapsed={}",
									getClass().getSimpleName(), key, getInputCount(), qtLut.size(),
									getOutputCount(), watch.stopAndGetElpasedTimeString());
				}
			}
		};
	}
}