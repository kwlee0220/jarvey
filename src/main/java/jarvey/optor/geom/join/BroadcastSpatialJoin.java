package jarvey.optor.geom.join;


import java.util.Iterator;
import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.optor.FlatMapIterator;
import jarvey.optor.RecordSerDe;
import jarvey.optor.geom.GeometryRDDFunction;
import jarvey.optor.geom.SpatialRelation;
import jarvey.quadtree.LeafNode;
import jarvey.quadtree.PointerPartition;
import jarvey.support.RecordLite;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.StopWatch;
import utils.geo.util.CoordinateTransform;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class BroadcastSpatialJoin extends GeometryRDDFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(BroadcastSpatialJoin.class);

	protected final JarveySchema m_paramSchema;
	protected final Broadcast<List<Row>> m_params;
	protected final SpatialJoinOptions m_opts;

	protected GeometryColumnInfo m_paramGeomColInfo;
	protected int m_paramGeomColIdx;

	private transient SpatialJoinMatcher<EnvelopedRecordLite> m_matcher;
	private transient SpatialRelation m_joinExpr;
	private transient CoordinateTransform m_coordsTrans;
	
	abstract protected JarveySchema initializeJoin(JarveySchema inputSchema);
	abstract protected FStream<RecordLite> joinWithMatchingInners(EnvelopedRecordLite outer,
																FStream<EnvelopedRecordLite> inners);
	
	public BroadcastSpatialJoin(JarveySchema paramSchema, Broadcast<List<Row>> params,
								SpatialJoinOptions opts) {
		m_paramSchema = paramSchema;
		m_params = params;
		m_opts = opts;
	}

	@Override
	protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
		m_paramGeomColInfo = m_paramSchema.getDefaultGeometryColumnInfo();
		if ( m_paramGeomColInfo == null ) {
			String details = String.format("inner dataset does not have Geometry column: op=%s", this);
			throw new IllegalArgumentException(details);
		}
		m_paramGeomColIdx = m_paramSchema.getDefaultGeometryColumn().getIndex();
		
		// 조인에 참여하는 두 SpatialDataFrame의 SRID가 동일한지 체크한다.
		int srid = getInputGeometryColumnInfo().getSrid();
		if ( srid != m_paramGeomColInfo.getSrid() ) {
			throw new IllegalArgumentException("input srid is not compatible to parameter srid: "
												+ srid + "<->" + m_paramGeomColInfo.getSrid());
		}
		
		JarveySchema outputSchema = initializeJoin(inputSchema);
		return outputSchema;
	}
	
	@Override
	protected Iterator<RecordLite> mapPartition(final int partIdx, Iterator<RecordLite> iter) {
		initializeTask();
		
		int srid = getInputGeometryColumnInfo().getSrid();
		m_coordsTrans = (srid != 4326) ? CoordinateTransform.getTransformToWgs84(srid) : null;
		
		m_matcher = SpatialJoinMatchers.from(m_opts.joinExpr());
		m_matcher.open(getInputSchema(), m_paramSchema);
		m_joinExpr = m_opts.joinExpr();
		
		// broadcast variable에서 inner dataframe record를 읽어 Qtree 기반 look-up table을 구성한다.
		//
		StopWatch lutWatch = StopWatch.start();
		RecordSerDe serde = RecordSerDe.of(m_paramSchema);
		FStream<EnvelopedRecordLite> broadcasted = FStream.from(m_params.getValue())
															.map(serde::deserialize)
															.map(rec -> toEnveloped(rec, m_paramGeomColIdx));
		QTreeLookupTable<EnvelopedRecordLite> lut = new QTreeLookupTable<>(broadcasted);
		if ( s_logger.isDebugEnabled() ) {
			lutWatch.stop();
			
			int nonEmptyCount = 0;
			int emptyCount = 0;
			for ( PointerPartition part:
					lut.getQuadTree().streamLeafNodes().map(LeafNode::getPartition) ) {
				if ( part.size() > 0 ) {
					++nonEmptyCount;
				}
				else {
					++emptyCount;
				}
			}
			s_logger.debug("[{}]: Lookup table has been built: count={}, nodes={} ({} + {}), elapsed={}",
							partIdx, lut.getDataList().size(),
							nonEmptyCount+emptyCount, nonEmptyCount, emptyCount,
							lutWatch.getElapsedMillisString());
		}
		
		int colIdx = getInputGeometryColumn().getIndex();
		FlatMapIterator<RecordLite, RecordLite> fiter = new FlatMapIterator<RecordLite, RecordLite>(iter) {
			@Override
			protected Iterator<RecordLite> apply(RecordLite rec) {
				EnvelopedRecordLite evRec = toEnveloped(rec, colIdx);
				FStream<EnvelopedRecordLite> matchingInners = findMatchingInnerRecords(evRec, lut);
				return joinWithMatchingInners(evRec, matchingInners).iterator();
			}

			@Override
			protected void onIteratorFinished() { }

			@Override
			protected String getHandlerString() {
				return String.format("%s", BroadcastSpatialJoin.this);
			}
		};
		fiter.setLogger(s_logger);
		return fiter;
	}

	private FStream<EnvelopedRecordLite> findMatchingInnerRecords(EnvelopedRecordLite outer,
																	QTreeLookupTable<EnvelopedRecordLite> lut) {
		final Geometry outerGeom = outer.getGeometry();
		return m_matcher.match(outer, lut)
						.filter(inner -> m_joinExpr.test(outerGeom, inner.getGeometry()));
	}
	
	private EnvelopedRecordLite toEnveloped(RecordLite record, int geomColIdx) {
		Geometry geom = record.getGeometry(geomColIdx);
		Envelope bounds = null;
		if ( geom != null && !geom.isEmpty() ) {
			bounds = geom.getEnvelopeInternal();
			bounds = m_coordsTrans != null ? m_coordsTrans.transform(bounds) : bounds;
		}
		return new EnvelopedRecordLite(record, geom, bounds);
	}
}
