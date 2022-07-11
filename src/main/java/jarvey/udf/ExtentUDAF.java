package jarvey.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.type.EnvelopeValue;
import jarvey.type.GeometryType;
import jarvey.type.GeometryValue;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExtentUDAF extends Aggregator<Row,Row,Row> {
	private static final long serialVersionUID = 1L;

	public static final StructType INPUT_SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("geometry", GeometryType.DATA_TYPE, false),
	});
	public static final Encoder<Row> INPUT_ENCODER = RowEncoder.apply(INPUT_SCHEMA);
	
	@Override
	public Row zero() {
		return toSingleEnvelopeRow(EnvelopeValue.empty());
	}

	@Override
	public Row reduce(Row buffer, Row input) {
		if ( input != null ) {
			Geometry geom = GeometryValue.deserialize(input.getAs(0));
			if ( geom != null ) {
				Envelope accum = EnvelopeValue.toEnvelope(buffer.getAs(0));
				Envelope envl = geom.getEnvelopeInternal();
				accum.expandToInclude(envl);
				
				return toSingleEnvelopeRow(new EnvelopeValue(accum));
			}
		}
		
		return buffer;
	}

	@Override
	public Row merge(Row buffer1, Row buffer2) {
		Envelope accum1 = EnvelopeValue.toEnvelope(buffer1.getAs(0));
		Envelope accum2 = EnvelopeValue.toEnvelope(buffer2.getAs(0));
		accum1.expandToInclude(accum2);

		return toSingleEnvelopeRow(new EnvelopeValue(accum1));
	}

	@Override
	public Row finish(Row reduction) {
		return reduction;
	}

	@Override
	public Encoder<Row> bufferEncoder() {
		return EnvelopeValue.ENCODER;
	}

	@Override
	public Encoder<Row> outputEncoder() {
		return EnvelopeValue.ENCODER;
	}
	
	private static final Row toSingleEnvelopeRow(EnvelopeValue envl) {
		return new GenericRow(new Object[]{ envl.toCoordinates() });
	}
}
