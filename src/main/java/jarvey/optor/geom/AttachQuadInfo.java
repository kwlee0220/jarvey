package jarvey.optor.geom;

import java.util.Arrays;
import java.util.List;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Lists;

import jarvey.SpatialDataFrame;
import jarvey.support.MapTile;
import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.Utilities;
import utils.geo.util.CoordinateTransform;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AttachQuadInfo extends GeometryFunction {
	private static final long serialVersionUID = 1L;
	private static final String ENVL_COL = SpatialDataFrame.COLUMN_ENVL4326;
	private static final String QID_COL = SpatialDataFrame.COLUMN_QUAD_IDS;
	
	private final long[] m_candidateQids;
	
	private int m_envlColIdx;
	private int m_qidsColIdx;
	
	private transient CoordinateTransform m_coordTrans = null;
	private transient MapTile[] m_tiles;
	private transient List<Long> m_qids;	// 객체 생성 횟수를 줄이기 위한 재사용용
	
	public AttachQuadInfo(long[] candidateQids) {
		Utilities.checkArgument(candidateQids != null && candidateQids.length > 0,
								"invalid candidate quid-ids: " + candidateQids);
		
		m_candidateQids = Arrays.stream(candidateQids).filter(q -> q != MapTile.OUTLIER_QID).toArray();
	}

	@Override
	protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
		JarveySchema outSchema
				= inputSchema.toBuilder()
							.addOrReplaceJarveyColumn(ENVL_COL, JarveyDataTypes.Envelope_Type)
							.addOrReplaceJarveyColumn(QID_COL, JarveyDataTypes.LongArray_Type)
							.build();
		
		m_envlColIdx = outSchema.getColumn(SpatialDataFrame.COLUMN_ENVL4326).getIndex();
		m_qidsColIdx = outSchema.getColumn(QID_COL).getIndex();
		
		return outSchema;
	}
	
	protected void initializeTask() {
		int srid = getInputGeometryColumnInfo().getSrid();
		m_coordTrans = (srid != 4326) ? CoordinateTransform.getTransformToWgs84("EPSG:" + srid) : null;
		
		m_tiles = FStream.of(m_candidateQids).map(MapTile::fromQuadId).toArray(MapTile.class);
		m_qids = Lists.newArrayList();
	}

	private static final Object[] OUTLIER_MEMBERS = new Object[]{MapTile.OUTLIER_QID};
	@Override
	protected RecordLite apply(Geometry geom, RecordLite inputRecord) {
		RecordLite output = RecordLite.of(getOutputSchema());
		inputRecord.copyTo(output);
		
		if ( geom != null ) {
			Envelope envl = geom.getEnvelopeInternal();
			envl = (m_coordTrans != null) ? m_coordTrans.transform(envl) : envl;
			Coordinate refPt = envl.centre();

			m_qids.clear();
			for ( int i =0; i < m_tiles.length; ++i ) {
				if ( m_tiles[i].intersects(envl) ) {
					if ( m_tiles[i].contains(refPt) ) {
						// primary qid인 경우에는 qids 리스트에 맨 처음에 넣는다.
						m_qids.add(0, m_candidateQids[i]);
					}
					else {
						m_qids.add(m_candidateQids[i]);
					}
				}
			}

			if ( m_qids.size() > 0 ) {
				output.set(m_envlColIdx, envl);
				output.set(m_qidsColIdx, m_qids.toArray(new Long[m_qids.size()]));
			}
			else {
				output.set(m_envlColIdx, envl);
				output.set(m_qidsColIdx, OUTLIER_MEMBERS);
			}
		}
		else {
			output.set(m_envlColIdx, null);
			output.set(m_qidsColIdx, OUTLIER_MEMBERS);
		}
		
		return output;
	}
}