package jarvey.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.type.GeometryBean;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataTypes;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SummarizeSpatialInfoUDAF extends Aggregator<Row,SpatialInfoSummaryBean,Row> {
	private static final long serialVersionUID = 1L;

	public static final StructType INPUT_SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("geometry", GeometryType.DATA_TYPE, false),
	});
	public static final Encoder<Row> INPUT_ENCODER = RowEncoder.apply(INPUT_SCHEMA);
	
	@Override
	public SpatialInfoSummaryBean zero() {
		return new SpatialInfoSummaryBean();
	}

	@Override
	public SpatialInfoSummaryBean reduce(SpatialInfoSummaryBean buffer, Row input) {
		if ( input != null ) {
			Geometry geom = GeometryBean.deserialize(input.getAs(0));
			if ( geom != null ) {
				++buffer.m_count;
				buffer.m_bounds.expandToInclude(geom.getEnvelopeInternal());
			}
			else {
				++buffer.m_emptyGeomCount;
			}
		}
		
		return buffer;
	}

	@Override
	public SpatialInfoSummaryBean merge(SpatialInfoSummaryBean buffer1, SpatialInfoSummaryBean buffer2) {
		buffer1.m_count += buffer2.m_count;
		buffer1.m_emptyGeomCount += buffer2.m_emptyGeomCount;
		
		Envelope accum1 = buffer1.m_bounds;
		Envelope accum2 = buffer2.m_bounds;
		accum1.expandToInclude(accum2);

		return buffer1;
	}

	@Override
	public Row finish(SpatialInfoSummaryBean reduction) {
		Double[] coords = JarveyDataTypes.Envelope_Type.serialize(reduction.m_bounds);
		return RowFactory.create(reduction.m_count, reduction.m_emptyGeomCount, coords);
	}

	@Override
	public Encoder<SpatialInfoSummaryBean> bufferEncoder() {
		return Encoders.bean(SpatialInfoSummaryBean.class);
	}
	
	private static final StructType OUTPUT_SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("record_count", DataTypes.LongType, false),
		DataTypes.createStructField("empty_geometry_count", DataTypes.LongType, false),
		DataTypes.createStructField("bounds", JarveyDataTypes.Envelope_Type.getSparkType(), false),
	});
	@Override
	public Encoder<Row> outputEncoder() {
		return RowEncoder.apply(OUTPUT_SCHEMA);
	}
}
