package jarvey.optor.geom.join;

import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.optor.geom.SafeDifference;
import jarvey.optor.geom.SafeUnion;
import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BroadcastSpatialDifferenceJoin extends BroadcastSpatialJoin {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(BroadcastSpatialDifferenceJoin.class);
	
	private transient SafeDifference m_difference;
	private transient SafeUnion m_union;
	
	public BroadcastSpatialDifferenceJoin(JarveySchema paramSchema, Broadcast<List<Row>> params,
											SpatialJoinOptions opts) {
		super(paramSchema, params, opts);
		
		setLogger(s_logger);
	}

	@Override
	protected JarveySchema initializeJoin(JarveySchema inputSchema) {
		return getInputSchema();
	}

	@Override
	protected void initializeTask() {
		GeometryType geomType = getInputGeometryColumnInfo().getDataType();
		Geometries dstType = getInputGeometryColumnInfo().getGeometries();

		m_difference = new SafeDifference(geomType.getGeometries()).setReduceFactor(2);
		m_union = new SafeUnion(dstType);
	}

	@Override
	protected FStream<RecordLite> joinWithMatchingInners(EnvelopedRecordLite outer,
														FStream<EnvelopedRecordLite> inners) {
		RecordLite result = outer.getRecord().duplicate();
		
		List<Geometry> innerGeoms = inners.map(EnvelopedRecordLite::getGeometry).toList();
		if ( innerGeoms.size() > 0 ) {
			Geometry outerGeom = outer.getGeometry();
			Geometry innerGeom = m_union.apply(innerGeoms);
			Geometry diffGeom = m_difference.apply(outerGeom, innerGeom);
			result.set(getInputGeometryColumnIndex(), diffGeom);
		}

		return FStream.of(result);
	}
	
	@Override
	public String toString() {
		return String.format("%s", getClass().getSimpleName());
	}
}
