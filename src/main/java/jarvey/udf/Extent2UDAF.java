package jarvey.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.type.EnvelopeBean;
import jarvey.type.EnvelopeType;
import jarvey.type.GeometryBean;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Extent2UDAF extends UserDefinedAggregateFunction {
	private static final long serialVersionUID = 1L;
	private static final EnvelopeType SERDE = JarveyDataTypes.Envelope_Type;

	@Override
	public StructType inputSchema() {
		return new StructType(new StructField[] {
			DataTypes.createStructField("geometry", GeometryType.DATA_TYPE, false),
		});
	}

	@Override
	public StructType bufferSchema() {
		return EnvelopeBean.ENCODER.schema();
	}

	@Override
	public DataType dataType() {
		return EnvelopeBean.DATA_TYPE;
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, new EnvelopeBean().getCoordinates());
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		Envelope envl = SERDE.deserialize(buffer.getAs(0));
		Geometry geom = GeometryBean.deserialize(input.getAs(0));
		
		envl.expandToInclude(geom.getEnvelopeInternal());
		buffer.update(0, SERDE.serialize(envl));
	}

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		Envelope accum1 = SERDE.deserialize(buffer1.getAs(0));
		Envelope accum2 = SERDE.deserialize(buffer2.getAs(0));
		accum1.expandToInclude(accum2);

		buffer1.update(0, SERDE.serialize(accum1));
	}

	@Override
	public Object evaluate(Row buffer) {
		return buffer.get(0);
	}
}
