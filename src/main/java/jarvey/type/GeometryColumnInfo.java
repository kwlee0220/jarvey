package jarvey.type;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.geotools.geometry.jts.Geometries;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeometryColumnInfo implements Serializable {
	private static final long serialVersionUID = 1L;

//	private static final Pattern PATTERN = Pattern.compile("(\\S+)\\s*\\(\\s*([0-9]+)?\\s*\\)");
	private static final Pattern PATTERN = Pattern.compile("(\\S+)\\s*\\((\\S+)\\s*:\\s*([0-9]+)?\\s*\\)");
	
	private final String m_name;
	private final GeometryType m_type;
	
	public GeometryColumnInfo(String colName, GeometryType colType) {
		Utilities.checkNotNullArgument(colName, "Geometry column name");
		Utilities.checkNotNullArgument(colType, "Geometry type");
		
		m_name = colName;
		m_type = colType;
	}
	
	public final String getName() {
		return m_name;
	}
	
	public final GeometryType getDataType() {
		return m_type;
	}
	
	public final int getSrid() {
		return m_type.getSrid();
	}
	
	public final Geometries getGeometries() {
		return m_type.getGeometries();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || getClass() != obj.getClass() ) {
			return false;
		}
		
		GeometryColumnInfo other = (GeometryColumnInfo)obj;
		return m_name.equalsIgnoreCase(other.m_name)
				&& Objects.equals(m_type, other.m_type);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_name.toLowerCase(), m_type);
	}
	
	public static GeometryColumnInfo fromString(String str) {
		Matcher matcher = PATTERN.matcher(str.trim());
		if ( !matcher.find() ) {
			throw new IllegalArgumentException(String.format("invalid: '%s'", str));
		}
		
		String geomName = matcher.group(1);
		String geomTypeStr = matcher.group(2);
		int srid = (matcher.groupCount() == 3 ) ? Integer.parseInt(matcher.group(3)) : 0;
		GeometryType geomType = GeometryType.fromString(geomTypeStr, srid);
		return new GeometryColumnInfo(geomName, geomType);
	}
	
	@Override
	public String toString() {
		return String.format("%s(%s:%d)", m_name, m_type.getGeometries(), m_type.getSrid());
	}
}
