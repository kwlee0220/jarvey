package jarvey.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.locationtech.jts.geom.Geometry;

import jarvey.type.GeometryValue;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UnionUDAF extends CollectUDAF {
	private static final long serialVersionUID = 1L;

	@Override
	public Row finish(Row reduction) {
		Geometry coll = GeometryValue.deserialize(super.finish(reduction).getAs(0));
		Geometry geom = coll.union();
		return new GenericRow(new Object[] {GeometryValue.serialize(geom)});
	}
}
