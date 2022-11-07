package jarvey.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import jarvey.type.GeometryArrayBean;
import jarvey.type.GeometryType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AccumGeometryUDAF<T> extends Aggregator<Row,GeometryArrayBean,T> {
	private static final long serialVersionUID = 1L;

	public static final StructType INPUT_SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("geometry", GeometryType.DATA_TYPE, false),
	});
	public static final Encoder<Row> INPUT_ENCODER = RowEncoder.apply(INPUT_SCHEMA);
	
	@Override
	public GeometryArrayBean zero() {
		return new GeometryArrayBean();
	}

	@Override
	public GeometryArrayBean reduce(GeometryArrayBean buffer, Row input) {
		byte[] wkb = input.getAs(0);
		buffer.add(wkb);
		
		return buffer;
	}

	@Override
	public GeometryArrayBean merge(GeometryArrayBean buffer1, GeometryArrayBean buffer2) {
		buffer1.addAll(buffer2.getWkbs());
		return buffer1;
	}

	@Override
	public Encoder<GeometryArrayBean> bufferEncoder() {
		return GeometryArrayBean.ENCODER;
	}
}
