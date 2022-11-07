package jarvey.type.temporal;

import java.util.Objects;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import jarvey.datasource.DatasetException;
import jarvey.type.JarveyDataType;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TemporalPointType extends JarveyDataType {
	private static final long serialVersionUID = 1L;

	public static final StructType SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("length", DataTypes.IntegerType, true),
		DataTypes.createStructField("first_ts", DataTypes.TimestampType, true),
		DataTypes.createStructField("last_ts", DataTypes.TimestampType, true),
		DataTypes.createStructField("xyt_compressed", DataTypes.BinaryType, true)
	});
//	public static final StructType SCHEMA = new StructType(new StructField[] {
//		DataTypes.createStructField("xs", DataTypes.createArrayType(DataTypes.DoubleType), true),
//		DataTypes.createStructField("ys", DataTypes.createArrayType(DataTypes.DoubleType), true),
//		DataTypes.createStructField("ts", DataTypes.createArrayType(DataTypes.LongType), true)
//	});
	private static final DataType SPARK_TYPE = SCHEMA;
	public static final Encoder<Row> ENCODER = RowEncoder.apply(SCHEMA);
	
	private static final TemporalPointType SINGLETON = new TemporalPointType();
	public static final TemporalPointType get() {
		return SINGLETON;
	}
	
	protected TemporalPointType() {
		super(SPARK_TYPE);
	}

	@Override
	public Class<?> getJavaClass() {
		return TemporalPoint.class;
	}
	
	@Override
	public  Row serialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof TemporalPoint ) {
			TemporalPoint tp = (TemporalPoint)value;
			return tp.toRow();
		}
		else {
			throw new DatasetException("invalid TemporalPoint: " + value);
		}
	}

	@Override
	public TemporalPoint deserialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof Row ) {
			return TemporalPoint.from((Row)value);
		}
		else {
			throw new DatasetException("invalid serialized value for TemporalPoint: " + value);
		}
	}
	
	@Override
	public String toString() {
		return "TemporalPoint";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof TemporalPointType) ) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(getSparkType());
	}
}
