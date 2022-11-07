package jarvey.udf;

import org.apache.spark.sql.Encoder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;

import jarvey.type.GeometryArrayBean;
import jarvey.type.GeometryBean;

import utils.geo.util.GeometryUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CollectUDAF extends AccumGeometryUDAF<GeometryBean> {
	private static final long serialVersionUID = 1L;

	@Override
	public GeometryBean finish(GeometryArrayBean reduction) {
		Geometry[] geoms = reduction.asGeometries();
		GeometryCollection coll = GeometryUtils.GEOM_FACT.createGeometryCollection(geoms);
		return GeometryBean.of(coll);
	}

	@Override
	public Encoder<GeometryBean> outputEncoder() {
		return GeometryBean.ENCODER;
	}
}
