/**
 * 
 */
package jarvey.type;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;

import scala.collection.mutable.WrappedArray;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class EnvelopeValue {
	private final Envelope m_envl;

	private static final EnvelopeValue EMPTY = new EnvelopeValue(new Envelope());
	public static final DataType DATA_TYPE = DataTypes.createArrayType(DataTypes.DoubleType, false);
	public static final StructType ROW_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("coords", DATA_TYPE, true),
	});
	public static final Encoder<Row> ENCODER = RowEncoder.apply(ROW_TYPE);
	
	public EnvelopeValue(Envelope envl) {
		m_envl = envl;
	}

	public static EnvelopeValue empty() {
		return EMPTY;
	}
	
	public static EnvelopeValue from(WrappedArray<Double> coords) {
		if ( coords != null ) {
			Envelope envl = new Envelope(coords.apply(0), coords.apply(1), coords.apply(2), coords.apply(3));
			return new EnvelopeValue(envl);
		}
		else {
			return new EnvelopeValue(null);
		}
	}
	
	public Double[] toCoordinates() {
		return toCoordinates(m_envl);
	}
	
	public static Double[] toCoordinates(Envelope envl) {
		if ( envl != null ) {
			return new Double[]{ envl.getMinX(), envl.getMaxX(), envl.getMinY(), envl.getMaxY()};
		}
		else {
			return null;
		}
	}
	
	public static Envelope toEnvelope(WrappedArray<Double> coords) {
		if ( coords != null ) {
			return new Envelope(coords.apply(0), coords.apply(1), coords.apply(2), coords.apply(3));
		}
		else {
			return null;
		}
	}
	
	public Envelope asEnvelope() {
		return m_envl;
	}
}
