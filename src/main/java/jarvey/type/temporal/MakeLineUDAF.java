package jarvey.type.temporal;

import java.util.List;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;

import jarvey.type.GeometryBean;
import jarvey.type.GeometryType;
import jarvey.udf.CoordinateArrayBean;

import utils.geo.util.GeometryUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MakeLineUDAF extends Aggregator<Row,CoordinateArrayBean,GeometryBean> {
	private static final long serialVersionUID = 1L;

	public static final StructType INPUT_SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("geometry", GeometryType.DATA_TYPE, false),
	});
	public static final Encoder<Row> INPUT_ENCODER = RowEncoder.apply(INPUT_SCHEMA);
	
	@Override
	public CoordinateArrayBean zero() {
		return new CoordinateArrayBean();
	}

	@Override
	public CoordinateArrayBean reduce(CoordinateArrayBean buffer, Row input) {
		Geometry geom = GeometryBean.deserialize(input.getAs(0));
		if ( geom != null ) {
			buffer.add(geom.getCoordinate());
		}
		
		return buffer;
	}

	@Override
	public CoordinateArrayBean merge(CoordinateArrayBean buffer1, CoordinateArrayBean buffer2) {
		buffer1.addAll(buffer2.getCoordinateList());
		return buffer1;
	}

	@Override
	public GeometryBean finish(CoordinateArrayBean reduction) {
		List<Coordinate> coordList = reduction.getCoordinateList();
		Coordinate[] coords = coordList.toArray(new Coordinate[coordList.size()]);
		
		LineString line = GeometryUtils.toLineString(coords);
		return GeometryBean.of(line);
	}
	
	@Override
	public Encoder<CoordinateArrayBean> bufferEncoder() {
		return CoordinateArrayBean.ENCODER;
	}

	@Override
	public Encoder<GeometryBean> outputEncoder() {
		return GeometryBean.ENCODER;
	}
}
