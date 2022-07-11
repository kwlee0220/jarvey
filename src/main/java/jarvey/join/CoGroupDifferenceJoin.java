package jarvey.join;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.locationtech.jts.geom.Geometry;

import jarvey.SpatialDataset;
import jarvey.support.SafeDifference;
import jarvey.type.GeometryValue;
import jarvey.type.JarveySchema;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class CoGroupDifferenceJoin extends CoGroupSpatialJoin {
	private static final long serialVersionUID = 1L;
	
	private transient SafeDifference m_difference;

	public CoGroupDifferenceJoin(SpatialDataset left, SpatialDataset right, SpatialJoinOptions opts) {
		super(left, right, opts);
	}
	
	@Override
	public JarveySchema getOutputJarveySchema() {
		return m_left;
	}

	@Override
	protected void setupContext() {
		super.setupContext();
		
		if ( m_difference == null ) {
			m_difference = new SafeDifference().setReduceFactor(0);;
		}
	}

	@Override
	protected FStream<Row> handleMatches(QidAttachedRow left, FStream<QidAttachedRow> matches) {
		List<QidAttachedRow> matchedRows = matches.toList();
		if ( matchedRows.size() == 0 ) {
			return FStream.empty();
		}
		
		Geometry geom = left.getGeometry();
		for ( QidAttachedRow right: matchedRows ) {
			geom = m_difference.apply(geom, right.getGeometry());
			if ( geom.isEmpty() ) {
				return FStream.empty();
			}
		}
		
		int idx = m_left.getDefaultGeometryColumn().getIndex();
		Row srcRow = left.getRow();
		Object[] tarValues = new Object[srcRow.length()];
		for ( int i =0; i < srcRow.length(); ++i ) {
			if ( i == idx ) {
				tarValues[i] = GeometryValue.serialize(geom);
			}
			else {
				tarValues[i] = srcRow.get(i);
			}
		}
		Row updated = new GenericRow(tarValues);
		
		return FStream.of(updated);
	}
}
