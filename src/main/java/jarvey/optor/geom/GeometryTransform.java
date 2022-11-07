package jarvey.optor.geom;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Geometry;

import jarvey.support.RecordLite;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryTransform extends GeometryFunction {
	private static final long serialVersionUID = 1L;
	
	private final GeomOpOptions m_opts;
	
	protected int m_outColIdx = -1;
	protected GeometryColumnInfo m_outputGcInfo;
	
	abstract protected GeometryType toOutputGeometryType(GeometryType geomType, JarveySchema inputSchema);
	abstract protected Geometry transform(@Nullable Geometry geom, RecordLite inputRecord);
	
	protected GeometryTransform(GeomOpOptions opts) {
		Utilities.checkNotNullArgument(opts, "GeomOpOptions");

		m_opts = opts;
	}
	
	public GeometryColumnInfo getOutputGeometryColumnInfo() {
		return m_outputGcInfo;
	}

	@Override
	protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
		String outCol = m_opts.outputColumn().getOrElse(inputSchema.assertDefaultGeometryColumn().getName().get());
		GeometryType outputGeomType = toOutputGeometryType(geomType, inputSchema);
		JarveySchema outSchema = inputSchema.toBuilder()
											.addOrReplaceJarveyColumn(outCol, outputGeomType)
											.build();
		m_outColIdx = outSchema.getColumn(outCol).getIndex();
		m_outputGcInfo = new GeometryColumnInfo(outCol, outputGeomType);
		
		return outSchema;
	}

	@Override
	protected RecordLite apply(Geometry geom, RecordLite inputRecord) {
		RecordLite output = RecordLite.of(getOutputSchema());
		inputRecord.copyTo(output);
		
		Geometry outGeom = transform(geom, inputRecord);
		output.set(m_outColIdx, outGeom);
		
		return output;
	}
	
	@Override
	public String toString() {
		return String.format("%s: %s", getClass().getSimpleName(), m_opts);
	}
}
