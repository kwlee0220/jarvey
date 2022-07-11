package jarvey.type;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeometryColumnInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final Pattern PATTERN = Pattern.compile("(\\S+)\\s*\\(\\s*([0-9]+)?\\s*\\)");
	
	private final String m_name;
	private final int m_srid;
	
	public GeometryColumnInfo(String colName, int srid) {
		Utilities.checkNotNullArgument(colName, "column name");
		Utilities.checkNotNullArgument(srid, "SRID");
		
		m_name = colName;
		m_srid = srid;
	}
	
	public final String name() {
		return m_name;
	}
	
	public final int srid() {
		return m_srid;
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
			&& Objects.equals(m_srid, other.m_srid);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_name.toLowerCase(), m_srid);
	}
	
	public static GeometryColumnInfo fromString(String str) {
		Matcher matcher = PATTERN.matcher(str.trim());
		if ( !matcher.find() ) {
			throw new IllegalArgumentException(String.format("invalid: '%s'", str));
		}
		
		int srid = (matcher.groupCount() == 2 ) ? Integer.parseInt(matcher.group(2)) : 0;
		return new GeometryColumnInfo(matcher.group(1), srid);
	}
	
	@Override
	public String toString() {
		return String.format("%s(%d)", m_name, m_srid);
	}
}
