package jarvey.optor.geom.join;

import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.optor.geom.SafeIntersection;
import jarvey.optor.geom.SafeUnion;
import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BroadcastArcClip extends BroadcastSpatialJoin {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(BroadcastArcClip.class);
	
	private transient SafeIntersection m_intersection;
	private transient SafeUnion m_union;
	
	public BroadcastArcClip(JarveySchema paramSchema, Broadcast<List<Row>> params,
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

		m_intersection = new SafeIntersection(geomType).setReduceFactor(0);
		m_union = new SafeUnion(dstType);
	}

	@Override
	protected FStream<RecordLite> joinWithMatchingInners(EnvelopedRecordLite outer,
														FStream<EnvelopedRecordLite> inners) {
		RecordLite result = outer.getRecord().duplicate();
		
		Geometry outerGeom = outer.getGeometry();
		if ( outerGeom == null || outerGeom.isEmpty() ) {
			return FStream.of(result);
		}
		
		List<Geometry> clips = inners.map(EnvelopedRecordLite::getGeometry)
									.filter(innerGeom -> innerGeom != null && !innerGeom.isEmpty())
									.map(innerGeom -> m_intersection.apply(outerGeom, innerGeom))
									.toList();
		if ( clips.size() > 0 ) {
			result.set(getInputGeometryColumnIndex(), m_union.apply(clips));
			return FStream.of(result);
		}
		else {
			return FStream.empty();
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s", getClass().getSimpleName());
	}
}
