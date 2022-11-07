package jarvey.junk;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.SpatialDataFrame;
import jarvey.optor.FlatMapIterator;
import jarvey.optor.RecordSerDe;
import jarvey.optor.geom.SpatialRelation;
import jarvey.optor.geom.join.QTreeLookupTable;
import jarvey.optor.geom.join.QuadInfoRecord;
import jarvey.optor.geom.join.QuadInfoRecordSerDe;
import jarvey.optor.geom.join.SpatialJoinMatcher;
import jarvey.optor.geom.join.SpatialJoinMatchers;
import jarvey.optor.geom.join.SpatialJoinOptions;
import jarvey.support.RecordLite;
import jarvey.type.JarveySchema;

import scala.Tuple2;
import utils.StopWatch;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class CoGroupSpatialJoin implements FlatMapFunction<Tuple2<Long, Tuple2<Iterable<Row>,Iterable<Row>>>,Row> {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(CoGroupSpatialJoin.class);
	
	protected final JarveySchema m_leftSchema;
	protected final JarveySchema m_rightSchema;
	protected final SpatialJoinOptions m_opts;
	
	private transient SpatialJoinMatcher<QuadInfoRecord> m_matcher;
	
	public abstract JarveySchema getOutputSchema();
	protected abstract RecordLite combine(QuadInfoRecord left, QuadInfoRecord right);
	
	protected CoGroupSpatialJoin(JarveySchema leftSchema, JarveySchema rightSchema, SpatialJoinOptions opts) {
		m_leftSchema = leftSchema;
		m_rightSchema = rightSchema;
		m_opts = opts;
		
		// 조인에 참여하는 DataFrame 모두 QuadIinfo 정보가 존재하는지 확인한다.
		assertQuadInfo(m_leftSchema);
		assertQuadInfo(m_rightSchema);
		
		int leftSrid = m_leftSchema.assertDefaultGeometryColumnInfo().getSrid();
		int rightSrid = m_rightSchema.assertDefaultGeometryColumnInfo().getSrid();
		if ( leftSrid != rightSrid ) {
			String msg = String.format("Left SpatialDataFrame's srid is not compatible to that of "
										+ "right SpatialDataFrame: %d <-> %d", leftSrid, rightSrid);
			throw new IllegalArgumentException(msg);
		}
	}
	
	@Override
	public Iterator<Row> call(Tuple2<Long, Tuple2<Iterable<Row>, Iterable<Row>>> chunk) throws Exception {
		final StopWatch watch = StopWatch.start();
		
		long key = chunk._1;
		Iterable<Row> left = chunk._2._1;
		Iterable<Row> right = chunk._2._2;
		
		SpatialRelation joinExpr = m_opts.joinExpr();
		m_matcher = SpatialJoinMatchers.from(joinExpr);
		m_matcher.open(m_leftSchema, m_rightSchema);
		
		// right row-set을 이용하여 lookup-table을 생성
		QuadInfoRecordSerDe rightSerde = QuadInfoRecordSerDe.of(m_rightSchema);
		FStream<QuadInfoRecord> rightRecs = FStream.from(right).map(rightSerde::deserialize);
		final QTreeLookupTable<QuadInfoRecord> qtLut = new QTreeLookupTable<>(key, rightRecs);
		
		// left row-set에 포함된 각 레코드에 대해 join 시도.
		final QuadInfoRecordSerDe leftSerde = QuadInfoRecordSerDe.of(m_leftSchema);
		final RecordSerDe outputSerde = RecordSerDe.of(getOutputSchema());
		return new FlatMapIterator<Row, Row>(left.iterator()) {
			@Override
			protected Iterator<Row> apply(Row row) {
				QuadInfoRecord outer = leftSerde.deserialize(row);
				return handleInnerMatchingRecords(outer, m_matcher.match(outer, qtLut))
							.map(outputSerde::serialize)
							.iterator();
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
	
	protected FStream<RecordLite> handleInnerMatchingRecords(QuadInfoRecord outer,
															FStream<QuadInfoRecord> matchingInners) {
		return matchingInners.map(inner -> combine(outer, inner));
	}
	
	private void assertQuadInfo(JarveySchema jschema) {
		IllegalArgumentException error = new IllegalArgumentException("QuadInfo is not attached: schema=" + jschema);
		
		jschema.assertDefaultGeometryColumn().getIndex();
		jschema.findColumn(SpatialDataFrame.COLUMN_ENVL4326).getOrThrow(() -> error);
		jschema.findColumn(SpatialDataFrame.COLUMN_PARTITION_QID).getOrThrow(() -> error);
		jschema.findColumn(SpatialDataFrame.COLUMN_QUAD_IDS).getOrThrow(() -> error);
	}
}