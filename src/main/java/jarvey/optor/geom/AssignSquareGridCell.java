package jarvey.optor.geom;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import jarvey.support.GeoUtils;
import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.GridCell;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.Size2d;
import utils.Size2i;
import utils.Utilities;
import utils.geo.util.GeoClientUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssignSquareGridCell extends UnaryGeometryRecordFlatMapFunction {
	private static final long serialVersionUID = 1L;
	
	private final SquareGrid m_grid;
	private final boolean m_assignOutside;

	private Envelope m_bounds;
	private Size2i m_gridSize;
	private Polygon m_universePolygon;
	private int m_outputColIdx;
	
	public AssignSquareGridCell(SquareGrid grid, boolean assignOutside) {
		Utilities.checkNotNullArgument(grid, "SquareGrid is null");
		
		m_grid = grid;
		m_assignOutside = assignOutside;
	}

	@Override
	protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
		m_bounds = m_grid.getGridBounds();
		Size2d cellSize = m_grid.getCellSize();
		m_gridSize = new Size2i((int)Math.ceil(m_bounds.getWidth() / cellSize.getWidth()),
								(int)Math.ceil(m_bounds.getHeight() / cellSize.getHeight()));
		
		m_universePolygon = GeoUtils.toPolygon(m_bounds);
		m_outputColIdx = inputSchema.getColumnCount();
		
		GeometryType gridCellGeomType = GeometryType.of(Geometries.POLYGON, geomType.getSrid());
		JarveySchema outSchema = inputSchema.toBuilder()
											.addJarveyColumn("cell_geom", gridCellGeomType)
											.addJarveyColumn("cell_pos", JarveyDataTypes.GridCell_Type)
											.addJarveyColumn("cell_id", JarveyDataTypes.Long_Type)
											.build();
		return outSchema;
	}

	@Override
	protected Iterator<RecordLite> apply(@Nullable Geometry geom, RecordLite inputRecord) {
		if ( geom != null && !geom.isEmpty() && m_universePolygon.intersects(geom) ) {
			return FStream.from(findCover(geom))
							.map(info -> {
								RecordLite output = RecordLite.of(getOutputSchema());
								inputRecord.copyTo(output);
								output.set(m_outputColIdx, info.m_geom);
								output.set(m_outputColIdx+1, info.m_pos);
								output.set(m_outputColIdx+2, info.m_ordinal);
								
								return output;
							})
							.iterator();
		}
		else if ( !m_assignOutside ) {
			return Collections.emptyIterator();
		}
		else {
			RecordLite output = RecordLite.of(getOutputSchema());
			inputRecord.copyTo(output);
			
			return Iterators.singletonIterator(output);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: geom=%s, grid=%s", getClass().getSimpleName(),
							getInputGeometryColumnInfo(), m_grid);
	}
	
	private static class CellInfo {
		private final Geometry m_geom;
		private final GridCell m_pos;
		private final long m_ordinal;
		
		private CellInfo(Geometry geom, GridCell pos, long ordinal) {
			m_geom = geom;
			m_pos = pos;
			m_ordinal = ordinal;
		}
		
		@Override
		public String toString() {
			return String.format("%s:%d", m_pos, m_ordinal);
		}
	}
	
	private List<CellInfo> findCover(Geometry geom) {
		Envelope envl = geom.getEnvelopeInternal();
		double width = m_grid.getCellSize().getWidth();
		double height = m_grid.getCellSize().getHeight();
		
		int minX = (int)Math.floor((envl.getMinX() - m_bounds.getMinX()) / width);
		int minY = (int)Math.floor((envl.getMinY() - m_bounds.getMinY()) / height);
		int maxX = (int)Math.floor((envl.getMaxX() - m_bounds.getMinX()) / width);
		int maxY = (int)Math.floor((envl.getMaxY() - m_bounds.getMinY()) / height);
		
		List<CellInfo> cover = Lists.newArrayList();
		for ( int y = minY; y <= maxY; ++y ) {
			for ( int x = minX; x <= maxX; ++x ) {
				double x1 = m_bounds.getMinX() + (x * width);
				double y1 = m_bounds.getMinY() + (y * height);
				Envelope cellEnvl = new Envelope(x1, x1 + width, y1, y1 + height);
				Polygon poly = GeoClientUtils.toPolygon(cellEnvl);
				if ( poly.intersects(geom) ) {
					long ordinal = y * (m_gridSize.getWidth()) + x;
					
					cover.add(new CellInfo(poly, new GridCell(x,y), ordinal));
				}
			}
		}
		
		return cover;
	}
}
