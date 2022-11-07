package jarvey.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.type.EnvelopeBean;
import jarvey.type.GeometryBean;
import jarvey.type.GeometryType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExtentUDAF extends Aggregator<Row,EnvelopeBean,EnvelopeBean> {
	private static final long serialVersionUID = 1L;

	public static final StructType INPUT_SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("geometry", GeometryType.DATA_TYPE, false),
	});
	public static final Encoder<Row> INPUT_ENCODER = RowEncoder.apply(INPUT_SCHEMA);
	
	@Override
	public EnvelopeBean zero() {
		return new EnvelopeBean();
	}

	@Override
	public EnvelopeBean reduce(EnvelopeBean buffer, Row input) {
		if ( input != null ) {
			Geometry geom = GeometryBean.deserialize(input.getAs(0));
			if ( geom != null ) {
				Envelope accum = buffer.asEnvelope();
				Envelope envl = geom.getEnvelopeInternal();
				accum.expandToInclude(envl);
				buffer.update(accum);
				
				return buffer;
			}
		}
		
		return buffer;
	}

	@Override
	public EnvelopeBean merge(EnvelopeBean buffer1, EnvelopeBean buffer2) {
		Envelope accum1 = buffer1.asEnvelope();
		Envelope accum2 = buffer2.asEnvelope();
		accum1.expandToInclude(accum2);

		return buffer1;
	}

	@Override
	public EnvelopeBean finish(EnvelopeBean reduction) {
		return new EnvelopeBean(reduction.getCoordinates());
	}

	@Override
	public Encoder<EnvelopeBean> bufferEncoder() {
		return EnvelopeBean.ENCODER;
	}

	@Override
	public Encoder<EnvelopeBean> outputEncoder() {
		return EnvelopeBean.ENCODER;
	}
	
	public DataType[] toOutputDataTypes(StructType schema) {
		return new DataType[]{schema.fields()[0].dataType()};
	}
	
//	private static Row toSingleEnvelopeRow(EnvelopeValue envl) {
//		return new GenericRow(new Object[]{envl.toSparkValue()});
//	}
}
