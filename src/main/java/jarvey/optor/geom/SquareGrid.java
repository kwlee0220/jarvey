package jarvey.optor.geom;

import java.io.Serializable;
import java.util.Map;

import org.locationtech.jts.geom.Envelope;

import utils.CSV;
import utils.Size2d;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.KeyValue;
import utils.geo.util.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SquareGrid implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final Envelope m_gridBounds;
	private final Size2d m_cellSize;
	
	public SquareGrid(Envelope bounds, Size2d cellSize) {
		Utilities.checkNotNullArgument(bounds, "Universe Envelope should not be null");
		Utilities.checkNotNullArgument(cellSize, "Grid cell size should not be null");
		
		m_gridBounds = bounds;
		m_cellSize = cellSize;
	}
	
	public Envelope getGridBounds() {
		return m_gridBounds;
	}
	
	public Size2d getCellSize() {
		return m_cellSize;
	}
	
	@Override
	public String toString() {
		return String.format("bounds=%s;cell=%s",
							GeoClientUtils.toString(m_gridBounds), toString(m_cellSize));
	}
	
	private String toString(Size2d size) {
		return String.format("%sx%s", UnitUtils.toMeterString(size.getWidth()),
										UnitUtils.toMeterString(size.getHeight()));
	}
	
	public static SquareGrid parseString(String expr) {
		Utilities.checkNotNullArgument(expr, "SquareGrid string is null");
	
		Map<String,String> kvMap = CSV.parseCsv(expr, ';')
										.map(KeyValue::parse)
										.toMap(KeyValue::key, KeyValue::value);
		
		String cellExpr = kvMap.get("cell");
		if ( cellExpr == null ) {
			throw new IllegalArgumentException("cell is absent: expr=" + expr);
		}
		Size2d cell = Size2d.fromString(cellExpr);
		
		String boundsExpr = kvMap.get("bounds");
		if ( boundsExpr != null ) {
			return new SquareGrid(GeoClientUtils.parseEnvelope(boundsExpr).get(), cell);
		}
		
		throw new IllegalArgumentException("invalid SquareGrid string: " + expr);
	}
}
