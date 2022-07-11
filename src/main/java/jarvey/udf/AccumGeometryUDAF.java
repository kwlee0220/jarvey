package jarvey.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import jarvey.type.GeometryType;
import jarvey.type2.GeometryArrayValue;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AccumGeometryUDAF extends Aggregator<Row,Row,Row> {
	private static final long serialVersionUID = 1L;

	public static final StructType INPUT_SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("geometry", GeometryType.DATA_TYPE, false),
	});
	public static final Encoder<Row> INPUT_ENCODER = RowEncoder.apply(INPUT_SCHEMA);
	public static final Encoder<Row> BUFFER_ENCODER = RowEncoder.apply(GeometryArrayValue.ROW_TYPE);
	public static final Encoder<Row> OUTPUT_ENCODER = RowEncoder.apply(GeometryArrayValue.ROW_TYPE);
	
	@Override
	public Row zero() {
		return new GenericRow(new Object[] {new GeometryArrayValue()});
	}

	@Override
	public Row reduce(Row buffer, Row input) {
		byte[] wkb = input.getAs(0);
		GeometryArrayValue added = GeometryArrayValue.from(buffer.getAs(0)).add(wkb);
		return new GenericRow(new Object[] {added});
	}

	@Override
	public Row merge(Row buffer1, Row buffer2) {
		GeometryArrayValue geom1 = GeometryArrayValue.from(buffer1.getAs(0));
		GeometryArrayValue geom2 = GeometryArrayValue.from(buffer2.getAs(0));
		return new GenericRow(new Object[] {GeometryArrayValue.concat(geom1, geom2)});
	}

	@Override
	public Row finish(Row reduction) {
		return reduction;
	}

	@Override
	public Encoder<Row> bufferEncoder() {
		return BUFFER_ENCODER;
	}

	@Override
	public Encoder<Row> outputEncoder() {
		return OUTPUT_ENCODER;
	}
}
