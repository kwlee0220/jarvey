package jarvey.optor.geom;

import java.util.Iterator;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.LoggerFactory;

import jarvey.optor.FlatMapIterator;
import jarvey.support.RecordLite;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class UnaryGeometryRecordFlatMapFunction extends GeometryRDDFunction {
	private static final long serialVersionUID = 1L;

	abstract protected Iterator<RecordLite> apply(@Nullable Geometry geom, RecordLite inputRecord);

	@Override
	protected Iterator<RecordLite> mapPartition(int partIdx, Iterator<RecordLite> iter) {
		final int colIdx = getInputGeometryColumn().getIndex();
		FlatMapIterator<RecordLite,RecordLite> fiter = new FlatMapIterator<RecordLite,RecordLite>(iter) {
			@Override
			protected Iterator<RecordLite> apply(RecordLite input) {
				Geometry geom = input.getGeometry(colIdx);
				return UnaryGeometryRecordFlatMapFunction.this.apply(geom, input);
			}
			
			@Override
			protected String getHandlerString() {
				return UnaryGeometryRecordFlatMapFunction.this.toString();
			}
		};
		fiter.setLogger(LoggerFactory.getLogger(getClass()));
		return fiter;
	}
}
