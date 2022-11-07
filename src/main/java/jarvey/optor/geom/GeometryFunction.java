package jarvey.optor.geom;

import java.util.Iterator;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Geometry;

import jarvey.optor.MapIterator;
import jarvey.support.RecordLite;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryFunction extends GeometryRDDFunction {
	private static final long serialVersionUID = 1L;

	abstract protected RecordLite apply(@Nullable Geometry geom, RecordLite inputRecord);
	protected void initializeTask() { };

	@Override
	protected Iterator<RecordLite> mapPartition(int partIdx, Iterator<RecordLite> iter) {
		initializeTask();
		
		final int colIdx = getInputGeometryColumn().getIndex();
		MapIterator<RecordLite,RecordLite> outIter = new MapIterator<RecordLite,RecordLite>(iter) {
			@Override
			protected RecordLite apply(RecordLite input) {
				Geometry geom = input.getGeometry(colIdx);
				return GeometryFunction.this.apply(geom, input);
			}
			
			@Override
			protected String getHandlerString() {
				return GeometryFunction.this.toString();
			}
		};
		outIter.setLogger(getLogger());
		outIter.start();
		
		return outIter;
	}
}
