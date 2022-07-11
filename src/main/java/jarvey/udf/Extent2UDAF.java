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

import jarvey.type.EnvelopeValue;
import jarvey.type.GeometryType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Extent2UDAF extends UserDefinedAggregateFunction {
	private static final long serialVersionUID = 1L;

	@Override
	public StructType inputSchema() {
		return new StructType(new StructField[] {
			DataTypes.createStructField("geometry", GeometryType.DATA_TYPE, false),
		});
	}

	@Override
	public StructType bufferSchema() {
		return (StructType)EnvelopeValue.ROW_TYPE;
	}

	@Override
	public DataType dataType() {
		return EnvelopeValue.DATA_TYPE;
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, new Envelope());
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		Envelope envl = buffer.<Envelope>getAs(0);
		Geometry geom = input.<Geometry>getAs(0);
		
		envl.expandToInclude(geom.getEnvelopeInternal());
		buffer.update(0, envl);
	}

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		Envelope accum = buffer1.<Envelope>getAs(0);
		Envelope envl = buffer2.<Envelope>getAs(0);
		accum.expandToInclude(envl);

		buffer1.update(0, accum);
	}

	@Override
	public Object evaluate(Row buffer) {
		return buffer.<Envelope>getAs(0);
	}
}
