package jarvey.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;

import jarvey.type.GeometryType;
import jarvey.type.GeometryValue;
import jarvey.type2.GeometryArrayValue;
import utils.geo.util.GeometryUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CollectUDAF extends AccumGeometryUDAF {
	private static final long serialVersionUID = 1L;

	@Override
	public Row finish(Row reduction) {
		GeometryArrayValue rows = GeometryArrayValue.from(reduction.getAs(0));
		Geometry[] geoms = rows.getGeometries();
		GeometryCollection coll = GeometryUtils.GEOM_FACT.createGeometryCollection(geoms);
		return new GenericRow(new Object[] {new GeometryValue(coll)});
	}

	@Override
	public Encoder<Row> outputEncoder() {
		return RowEncoder.apply(GeometryType.ROW_TYPE);
	}
}
