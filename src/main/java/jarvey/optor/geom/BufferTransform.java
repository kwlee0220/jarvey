package jarvey.optor.geom;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import jarvey.support.RecordLite;
import jarvey.type.DataUtils;
import jarvey.type.GeometryType;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.Utilities;
import utils.func.FOption;
import utils.geo.util.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BufferTransform extends GeometryTransform {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(BufferTransform.class);

	private double m_distance = -1;
	private String m_distanceCol = null;
	private FOption<Integer> m_segmentCount = FOption.empty();
	
	private int m_distColIdx = -1;
	
	public BufferTransform(double distance, GeomOpOptions opts) {
		super(opts);
		Preconditions.checkArgument(Double.compare(distance, 0d) > 0,
									"invalid buffer distance: dist=" + distance);

		m_distance = distance;
		setLogger(LoggerFactory.getLogger(BufferTransform.class));
	}
	
	private BufferTransform(String distCol, GeomOpOptions opts) {
		super(opts);
		Utilities.checkNotNullArgument(distCol, "distance column is null");

		m_distanceCol = distCol;
		setLogger(s_logger);
	}

	@Override
	protected GeometryType toOutputGeometryType(GeometryType geomType, JarveySchema inputSchema) {
		if ( m_distanceCol != null ) {
			JarveyColumn distCol = inputSchema.findColumn(m_distanceCol).getOrNull();
			if ( distCol == null ) {
				String msg = String.format("invalid distance column: name=%s, schema=%s",
											m_distanceCol, inputSchema);
				throw new IllegalArgumentException(msg);
			}
			JarveyDataType distColType = distCol.getJarveyDataType();
			if ( distColType == JarveyDataTypes.Double_Type
				|| distColType == JarveyDataTypes.Float_Type 
				|| distColType == JarveyDataTypes.Integer_Type
				|| distColType == JarveyDataTypes.Long_Type 
				|| distColType == JarveyDataTypes.Short_Type 
				|| distColType == JarveyDataTypes.Byte_Type ) {
				m_distColIdx = distCol.getIndex();
			}
			else {
				String msg = String.format("invalid distance column: name=%s, type=%s",
											m_distanceCol, distCol.getJarveyDataType());
				throw new IllegalArgumentException(msg);
			}
		}
		
		switch ( geomType.getGeometries() ) {
			case POINT:
			case POLYGON:
			case LINESTRING:
				return JarveyDataTypes.PolygonType.newGeometryType(inputSchema.getSrid());
			default:
				return JarveyDataTypes.MultiPolygonType.newGeometryType(inputSchema.getSrid());
		}
	}

	@Override
	protected Geometry transform(Geometry geom, RecordLite inputRecord) {
		if ( geom == null || geom.isEmpty() ) {
			return geom;
		}
		
		double dist = m_distance;
		if ( dist < 0 ) {
			dist = DataUtils.asDouble(inputRecord.get(m_distColIdx));
		}

		Geometry buffered = m_segmentCount.isPresent()
							? BufferOp.bufferOp(geom, dist, m_segmentCount.get())
							: geom.buffer(dist);
		return GeoClientUtils.cast(buffered, getOutputGeometryColumnInfo().getGeometries());
	}
	
	@Override
	public String toString() {
		String distStr = (m_distanceCol != null)
						? String.format("distance_col=%s", m_distanceCol)
						: String.format("distance=%.2f", m_distance);
		
		return String.format("Buffer[%s]", distStr);
	}
}
