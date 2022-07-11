package jarvey.udf;

import java.util.Arrays;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

import com.google.common.collect.Lists;

import jarvey.type.GeometryType;
import jarvey.type.GeometryValue;
import jarvey.type2.GeometryArrayValue;
import utils.geo.util.GeometryUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MakeLineUDAF extends AccumGeometryUDAF {
	private static final long serialVersionUID = 1L;

	@Override
	public Row finish(Row reduction) {
		GeometryArrayValue rows = GeometryArrayValue.from(reduction.getAs(0));
		return new GenericRow(new Object[] {makeLine(rows)});
	}

	@Override
	public Encoder<Row> outputEncoder() {
		return RowEncoder.apply(GeometryType.ROW_TYPE);
	}
	
	static GeometryValue makeLine(GeometryArrayValue rows) {
		Coordinate[] coords = FStream.of(rows.getGeometries())
									.foldLeft(Lists.newArrayList(), (accum, geom) -> {
										if ( geom instanceof Point ) {
											accum.add(((Point)geom).getCoordinate());
										}
										else if ( geom instanceof LineString ) {
											accum.addAll(Arrays.asList(((LineString)geom).getCoordinates()));
										}
										return accum;
									})
									.toArray(new Coordinate[0]);
		LineString line = GeometryUtils.toLineString(coords);
		return new GeometryValue(line);
	}
}
