package jarvey.support;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import utils.func.Lazy;
import utils.geo.util.GeometryUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapTile implements Comparable<MapTile>, Serializable {
	private static final long serialVersionUID = 6102533639982982795L;
	public static final MapTile WORLD = new MapTile(0, 0, 0);
	public static final String QKEY_OUTLIER = "X";
	public static final Long QID_OUTLIER = Long.valueOf(-9L);
	
	private byte m_zoom;
	private int m_x;
	private int m_y;
	private String m_quadKey;	// lazy
	private Lazy<Envelope> m_bounds;
	
	public static MapTile fromLonLat(double lon, double lat, int zoom) {
		return new MapTile(zoom, toTileX(lon, zoom), toTileY(lat, zoom));
	}
	
	public static MapTile fromLonLat(Coordinate coord, int zoom) {
		return new MapTile(zoom, toTileX(coord.x, zoom), toTileY(coord.y, zoom));
	}
    
	public static MapTile fromQuadKey(String quadKey) {
		int tileX = 0;
		int tileY = 0;
		int zoom = quadKey.length();
		
		char[] chars = quadKey.toCharArray();
		for (int i = zoom; i > 0; i--) {
			int mask = 1 << (i - 1);
			switch (chars[zoom - i]) {
				case '0':
					break;
				case '1':
					tileX |= mask;
					break;
				case '2':
					tileY |= mask;
					break;
				case '3':
					tileX |= mask;
					tileY |= mask;
					break;
				default:
					throw new IllegalArgumentException("Invalid QuadKey digit sequence: "
														+ "quadkey=" + quadKey);
			}
		}

		return new MapTile(zoom, tileX, tileY, quadKey);
	}
	
	public static MapTile fromQuadId(long quadId) {
		return fromQuadKey(toQuadKey(quadId));
	}
	
	public static String toQuadKey(long quadId) {
		if ( quadId >= 0 ) {
			String qkey = Long.toString(quadId, 4);
			if ( qkey.length() > 31 ) {
				throw new IllegalArgumentException("invalid quad-id (too long): " + quadId);
			}
			
			return qkey;
		}
		else {
			return QKEY_OUTLIER;
		}
	}
	
	public static long toQuadId(String quadKey) {
		if ( quadKey.equalsIgnoreCase(QKEY_OUTLIER) ) {
			return QID_OUTLIER;
		}
		else {
			return Long.parseLong(quadKey, 4);
		}
	}
	
	public MapTile(int zoom, int x, int y) {
		this(zoom, x, y, null);
	}
	public MapTile(int zoom, int x, int y, String quadKey) {
		m_zoom = (byte)zoom;
		m_x = x;
		m_y = y;
		m_quadKey = quadKey;
		
		m_bounds = Lazy.of(() -> {
			Coordinate tl = toLonLat();
			Coordinate br = new MapTile(m_zoom, m_x+1, m_y+1).toLonLat();
			return new Envelope(tl, br);
		});
	}
	
	public int getZoom() {
		return m_zoom;
	}
	
	public int getX() {
		return m_x;
	}
	
	public int getY() {
		return m_y;
	}
    
	public String getQuadKey() {
		if ( m_quadKey == null ) {
			StringBuilder quadKey = new StringBuilder();
			for (int i = m_zoom; i > 0; --i) {
				char digit = '0';
				
				int mask = 1 << (i - 1);
				if ((m_x & mask) != 0) {
					++digit;
				}
				if ((m_y & mask) != 0) {
					++digit;
					++digit;
				}
				quadKey.append(digit);
			}
			m_quadKey = quadKey.toString();
		}
		
		return m_quadKey;
	}
	
	public long getQuadId() {
		return Long.parseLong(getQuadKey(), 4);
	}
	
	public boolean contains(Coordinate coord) {
		return getBounds().contains(coord);
	}
	
	public boolean contains(Envelope envl) {
		return getBounds().contains(envl);
	}
	
	public boolean intersects(Envelope envl) {
		return getBounds().intersects(envl);
	}
	
	public boolean intersects(Geometry geom) {
		return GeometryUtils.toPolygon(getBounds()).intersects(geom);
	}
	
	public Coordinate toLonLat() {
		return new Coordinate(toLon(), toLat());
	}
	
	public Envelope getBounds() {
		return m_bounds.get();
	}
	
	public MapTile next() {
		int upper = (int)Math.pow(2, m_zoom);
		if ( m_x + 1 < upper ) {
			return new MapTile(m_zoom, m_x+1, m_y);
		}
		else if ( m_y + 1 < upper ) {
			return new MapTile(m_zoom, 0, m_y+1);
		}
		else {
			return null;
		}
	}
	
	public MapTile previous() {
		int upper = (int)Math.pow(2, m_zoom);
		if ( m_x > 0 ) {
			return new MapTile(m_zoom, m_x-1, m_y);
		}
		else if ( m_y > 0 ) {
			return new MapTile(m_zoom, upper-1, m_y-1);
		}
		else {
			return null;
		}
	}
	
	public static List<MapTile> listTilesInRange(MapTile topLeft, MapTile bottomRight) {
		List<MapTile> tiles = Lists.newArrayList();
		for ( int y = topLeft.m_y; y <= bottomRight.m_y; ++y ) {
			for ( int x = topLeft.m_x; x <= bottomRight.m_x; ++x ) {
				tiles.add(new MapTile(topLeft.m_zoom, x, y));
			}
		}
		
		return tiles;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		MapTile other = (MapTile)obj;
		return m_x == other.m_x && m_y == other.m_y &&  m_zoom == other.m_zoom;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_x, m_y, m_zoom);
	}
	
	@Override
	public String toString() {
		return String.format("%d_%d_%d", m_zoom, m_x, m_y);
	}
	
	public static MapTile fromString(String str) {
		String[] parts = str.split("_");
		if ( parts.length != 3 ) {
			throw new IllegalArgumentException(String.format("invalid MapTile string: '%s'", str));
		}
		
		return new MapTile(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]),
							Integer.parseInt(parts[2]));
	}
	
	public static MapTile getSmallestContainingTile(Envelope envl, int maxLength) {
		String quadKey = "";
		MapTile tile = MapTile.fromQuadKey(quadKey);
		while ( true ) {
			String subKey = null;
			MapTile found = null;
			for ( int i =0; i < 4; ++i ) {
				MapTile sub = MapTile.fromQuadKey(subKey = quadKey + i);
				if ( sub.contains(envl) ) {
					found = sub;
					break;
				}
			}
			if ( found == null || tile.getZoom() == maxLength ) {
				return tile;
			}
			
			tile = found;
			quadKey = subKey;
		}
	}

	@Override
	public int compareTo(MapTile o) {
		if ( this == o ) {
			return 0;
		}
		
		int cmp = m_y - o.m_y;
		if ( cmp != 0 ) {
			return cmp;
		}
		
		return m_x - o.m_x;
	}
	
	public static List<String> subtractQuadKey(String quadKey1, String quadKey2) {
		Preconditions.checkArgument(quadKey1 != null, "QuadKey1 is null");
		Preconditions.checkArgument(quadKey2 != null, "QuadKey2 is null");
		Preconditions.checkArgument(quadKey2.startsWith(quadKey1),
									"quadKey1 is not greater than quadKey2");
		
		int depth = quadKey2.length()-quadKey1.length();
		if ( depth == 1 ) {
			return listSubQuadKeys(quadKey1)
					.filter(key -> !quadKey2.equals(key))
					.collect(Collectors.toList());
		}
		
		List<String> result = listSubQuadKeys(quadKey1)
									.filter(key -> !quadKey2.startsWith(key))
									.collect(Collectors.toList());
		String matchedQuadKey = listSubQuadKeys(quadKey1)
									.filter(key -> quadKey2.startsWith(key))
									.findFirst()
									.get();
		result.addAll(subtractQuadKey(matchedQuadKey, quadKey2));
		return result;
	}
	
//	public static List<String> listSubQuadKeys(String quadKey, int depth) {
//		List<String> quadKeys = Lists.newArrayList(quadKey);
//		for ( int i =0; i < depth; ++i ) {
//			quadKeys = quadKeys.stream()
//								.flatMap(MapTile::listSubQuadKeys)
//								.collect(Collectors.toList());
//		}
//		
//		return quadKeys;
//	}
	
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		m_zoom = ois.readByte();
		m_x = ois.readInt();
		m_y = ois.readInt();
		m_quadKey = ois.readUTF();
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.writeByte(m_zoom);
		oos.writeInt(m_x);
		oos.writeInt(m_y);
		oos.writeUTF(m_quadKey);
	}
	
	private static Stream<String> listSubQuadKeys(String quadKey) {
		return IntStream.range(0, 4)
						.mapToObj(idx -> quadKey + idx);
	}

	private double toLon() {
		double lon = m_x / Math.pow(2.0, m_zoom) * 360.0 - 180;
		if ( lon < -180 ) {
			return lon + 360d;
		}
		else {
			return lon;
		}
	}
	
	private double toLat() {
		double n = Math.PI - (2.0 * Math.PI * m_y) / Math.pow(2.0, m_zoom);
		return Math.toDegrees(Math.atan(Math.sinh(n)));
	}
	
	private static int toTileX(double lon, int zoom) {
		return (int)Math.floor((lon + 180.0) / 360.0 * Math.pow(2, zoom));
	}
	
	private static int toTileY(double lat, int zoom) {
		   return (int)(Math.floor((1.0 - Math.log(Math.tan(lat * Math.PI/180.0) + 1.0 / Math.cos(lat * Math.PI/180.0)) / Math.PI) / 2.0 * Math.pow(2.0, zoom)));
	}

}
