package jarvey.support;

import java.util.List;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Lists;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapTileConverter {
	private final int m_zoom;
	
	public MapTileConverter(int zoom) {
		m_zoom = zoom;
	}
	
	public MapTile getContainingMapTile(Coordinate coord4326) {
		return MapTile.fromLonLat(coord4326, m_zoom);
	}
	
	public static MapTile toMapTile(Coordinate coord4326, int zoom) {
		return new MapTileConverter(zoom).getContainingMapTile(coord4326);
	}
	
	public static List<MapTile> toMapTiles(Geometry geom4326, int zoom) {
		return new MapTileConverter(zoom).getContainingMapTiles(geom4326);
	}
	
	public static List<MapTile> toMapTiles(Envelope envl4326, int zoom) {
		return new MapTileConverter(zoom).getContainingMapTiles(envl4326);
	}
	
	public int getZoom() {
		return m_zoom;
	}
	
	public List<MapTile> getContainingMapTiles(Geometry geom4326) {
		List<MapTile> tiles = Lists.newArrayList();
		List<MapTile> candidates = getContainingMapTiles(geom4326.getEnvelopeInternal());
		for ( MapTile tile: candidates ) {
			if ( tile.intersects(geom4326) ) {
				tiles.add(tile);
			}
		}
		
		return tiles;
	}
	
	public List<MapTile> getContainingMapTiles(Envelope envl4326) {
		MapTile tlTile = MapTile.fromLonLat(envl4326.getMinX(), envl4326.getMaxY(), m_zoom);
		MapTile brTile = MapTile.fromLonLat(envl4326.getMaxX(), envl4326.getMinY(), m_zoom);
		
		return MapTile.listTilesInRange(tlTile, brTile);
	}
}
