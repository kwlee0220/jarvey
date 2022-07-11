package jarvey.type2.temporal;

import java.sql.Timestamp;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import jarvey.type.GeometryType;
import jarvey.type.GeometryValue;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildTemporalPointUDAF extends Aggregator<Row,Row,Row> {
	private static final long serialVersionUID = 1L;

	public static final StructType INPUT_SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("point", GeometryType.DATA_TYPE, false),
		DataTypes.createStructField("ts", DataTypes.TimestampType, false),
	});
	public static final Encoder<Row> INPUT_ENCODER = RowEncoder.apply(INPUT_SCHEMA);
	public static final Encoder<Row> OUTPUT_ENCODER = RowEncoder.apply(TemporalPoint.DATA_TYPE);
	public static final Encoder<Row> BUFFER_ENCODER = OUTPUT_ENCODER;
	
	@Override
	public Row zero() {
		return new TemporalPoint();
	}

	@Override
	public Row reduce(Row buffer, Row input) {
		GeometryValue geom = GeometryValue.from(input.getAs(0));
		Timestamp ts = input.getAs(1);
		TimedPoint timed = new TimedPoint(geom, ts.getTime());
		
		TemporalPoint temporal = TemporalPoint.fromRow(buffer);
		temporal.add(timed);
		
		return temporal;
	}

	@Override
	public Row merge(Row buffer1, Row buffer2) {
		TemporalPoint accum1 = TemporalPoint.fromRow(buffer1);
		TemporalPoint accum2 = TemporalPoint.fromRow(buffer2);
		accum1 = TemporalPoint.merge(accum1, accum2);
		
		return accum1;
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
