package jarvey.optor.geom;

import java.util.Iterator;

import org.locationtech.jts.geom.Geometry;

import jarvey.optor.MapIterator;
import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryPredicate extends GeometryRDDFunction {
	private static final long serialVersionUID = 1L;
	
	protected abstract boolean test(Geometry geom, RecordLite inputRecord);

	@Override
	protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
		return inputSchema;
	}

	@Override
	protected Iterator<RecordLite> mapPartition(final int partIdx, Iterator<RecordLite> iter) {
		initializeTask();
		
		final int colIdx = getInputGeometryColumn().getIndex();
		MapIterator<RecordLite,RecordLite> outIter = new MapIterator<RecordLite,RecordLite>(iter) {
			@Override
			protected RecordLite apply(RecordLite input) {
				Geometry geom = input.getGeometry(colIdx);
				return test(geom, input) ? input : null;
			}
			
			@Override
			protected String getHandlerString() {
				return GeometryPredicate.this.toString();
			}
		};
		outIter.setLogger(getLogger());
		outIter.start();
		
		return outIter;
	}
	
	public GeometryPredicate negate() {
		return GeometryPredicate.not(this); 
	}
	public GeometryPredicate and(GeometryPredicate pred2) {
		return GeometryPredicate.and(this, pred2); 
	}
	public GeometryPredicate or(GeometryPredicate pred2) {
		return GeometryPredicate.or(this, pred2); 
	}
	
	public static GeometryPredicate not(final GeometryPredicate pred) {
		return new GeometryPredicate() {
			private static final long serialVersionUID = 1L;

			@Override
			protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
				JarveySchema jschema = pred.initialize(geomType, inputSchema);
				return super.initialize(geomType, jschema);
			}
			
			@Override
			protected void initializeTask() {
				pred.initializeTask();
			}
			
			@Override
			protected boolean test(Geometry geom, RecordLite inputRecord) {
				return !pred.test(geom, inputRecord);
			}
			
			@Override
			public String toString() {
				return String.format("!%s", pred);
			}
		};
	}
	
	public static GeometryPredicate and(final GeometryPredicate pred1, final GeometryPredicate pred2) {
		return new GeometryPredicate() {
			private static final long serialVersionUID = 1L;

			@Override
			protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
				pred1.initialize(geomType, inputSchema);
				pred2.initialize(geomType, inputSchema);
				return super.initialize(geomType, inputSchema);
			}
			
			@Override
			protected void initializeTask() {
				pred1.initializeTask();
				pred2.initializeTask();
			}
			
			@Override
			protected boolean test(Geometry geom, RecordLite inputRecord) {
				return pred1.test(geom, inputRecord) && pred2.test(geom, inputRecord);
			}
			
			@Override
			public String toString() {
				return String.format("(%s AND %s)", pred1, pred2);
			}
		};
	}
	
	public static GeometryPredicate or(final GeometryPredicate pred1, final GeometryPredicate pred2) {
		return new GeometryPredicate() {
			private static final long serialVersionUID = 1L;

			@Override
			protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
				pred1.initialize(geomType, inputSchema);
				pred2.initialize(geomType, inputSchema);
				return super.initialize(geomType, inputSchema);
			}
			
			@Override
			protected void initializeTask() {
				pred1.initializeTask();
				pred2.initializeTask();
			}
			
			@Override
			protected boolean test(Geometry geom, RecordLite inputRecord) {
				return pred1.test(geom, inputRecord) || pred2.test(geom, inputRecord);
			}
			
			@Override
			public String toString() {
				return String.format("(%s OR %s)", pred1, pred2);
			}
		};
	}
}
