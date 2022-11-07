package jarvey.udf;

import java.util.Arrays;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.union.UnaryUnionOp;

import jarvey.type.GeometryArrayBean;
import jarvey.type.GeometryBean;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UnionUDAF extends AccumGeometryUDAF<GeometryBean> {
	private static final long serialVersionUID = 1L;

	@Override
	public GeometryArrayBean reduce(GeometryArrayBean buffer, Row input) {
		GeometryArrayBean reduced = super.reduce(buffer, input);
		if ( reduced.length() > 256 ) {
			Geometry unioned = unionAll(reduced);
			reduced.update(new Geometry[]{unioned});
		}
		
		return reduced;
	}

	@Override
	public GeometryArrayBean merge(GeometryArrayBean buffer1, GeometryArrayBean buffer2) {
		GeometryArrayBean merged = super.merge(buffer1, buffer2);
		if ( merged.length() > 256 ) {
			Geometry unioned = unionAll(merged);
			merged.update(new Geometry[]{unioned});
		}
		
		return merged;
	}

	@Override
	public GeometryBean finish(GeometryArrayBean reduction) {
		Geometry unioned = unionAll(reduction);
		return GeometryBean.of(unioned);
	}

	@Override
	public Encoder<GeometryBean> outputEncoder() {
		return GeometryBean.ENCODER;
	}
	
	private Geometry unionAll(GeometryArrayBean geomArray) {
		return UnaryUnionOp.union(Arrays.asList(geomArray.asGeometries()));
	}
}
