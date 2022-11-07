package jarvey.type;

import java.util.Objects;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import utils.Point2i;
import utils.Size2d;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GridCell implements Comparable<GridCell> {
	private final int m_x;
	private final int m_y;
	
	public static GridCell fromXY(int x, int y) {
		return new GridCell(x, y);
	}
	
	public GridCell(int x, int y) {
		m_x = x;
		m_y = y;
	}
	
	public int getColIdx() {
		return m_x;
	}
	
	public int getRowIdx() {
		return m_y;
	}
	
	public int getX() {
		return m_x;
	}
	
	public int getY() {
		return m_y;
	}
	
	public Point2i toPoint2i() {
		return new Point2i(m_x, m_y);
	}
	
	public GridCell next(int width) {
		if ( m_x + 1 < width ) {
			return new GridCell(m_x+1, m_y);
		}
		else {
			return new GridCell(0, m_y+1);
		}
	}
	
	public Envelope getEnvelope(Coordinate origin, Size2d cellSize) {
		Coordinate tl = new Coordinate(origin.x + m_x * cellSize.getWidth(),
										origin.y + m_y * cellSize.getHeight());
		Coordinate br = new Coordinate(tl.x + cellSize.getWidth(),
										tl.y + cellSize.getHeight());
		
		return new Envelope(tl, br);
	}

	@Override
	public int compareTo(GridCell o) {
		Utilities.checkNotNullArgument(o, "cell is null");
		
		int cmp = Integer.compare(m_x, o.m_x);
		if ( cmp != 0 ) {
			return cmp;
		}
		
		return Integer.compare(m_y, o.m_y);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		GridCell other = (GridCell)obj;
		return Objects.equals(m_x, other.m_x) && Objects.equals(m_y, other.m_y);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_x, m_y);
	}
	
	@Override
	public String toString() {
		return String.format("(%d,%d)", m_x, m_y);
	}
	
	public static GridCell fromString(String str) {
		str = str.substring(1, str.length()-1);
		String[] parts = str.split(",");
		
		return new GridCell(Integer.parseInt(parts[1]), Integer.parseInt(parts[0]));
	}
}
