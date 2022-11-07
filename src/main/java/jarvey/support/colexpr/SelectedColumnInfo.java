package jarvey.support.colexpr;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class SelectedColumnInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final String m_ns;
	private final String m_colName;
	private String m_alias = null;
	
	SelectedColumnInfo(String ns, String colName) {
		m_ns = ns;
		m_colName = colName;
	}
	
	public String getNamespace() {
		return m_ns;
	}
	
	public String getColumnName() {
		return m_colName;
	}
	
	public String getAlias() {
		return m_alias;
	}
	
	public void setAlias(String name) {
		m_alias = name;
	}
	
	public String getOutputColumnName() {
		return (m_alias != null) ? m_alias : m_colName;
	}
	
	public Column toColumnExpr(Map<String,Dataset<Row>> sdsMap) {
		Column expr = sdsMap.get(m_ns).col(m_colName);
		if ( m_alias != null ) {
			expr = expr.as(m_alias);
		}
		return expr;
	}
	
	@Override
	public String toString() {
		String ns = (m_ns.length() > 0) ? String.format("%s.", m_ns) : "";
		String al = (m_alias != null) ? String.format("(%s)", m_alias) : "";
		
		return String.format("%s%s%s", ns, m_colName, al);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj )  {
			return true;
		}
		else if ( obj == null || !(obj instanceof SelectedColumnInfo) ) {
			return false;
		}
		
		SelectedColumnInfo other = (SelectedColumnInfo)obj;
		return m_ns.equals(other.m_ns) && m_colName == other.m_colName;
	}
}
