package jarvey.type;

import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import utils.CIString;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveySchemaBuilder {
	private List<JarveyColumn> m_columns = Lists.newArrayList();
	private String m_defaultGeomColName;
	private long[] m_quadIds;
	
	public JarveySchema build() {
		return new JarveySchema(m_columns, m_defaultGeomColName, m_quadIds);
	}
	
	public JarveySchemaBuilder addJarveyColumn(String name, JarveyDataType jtype) {
		m_columns.add(new JarveyColumn(m_columns.size(), name, jtype));
		if ( m_defaultGeomColName == null && jtype.isGeometryType() ) {
			m_defaultGeomColName = name;
		}
		
		return this;
	}
	
	public JarveySchemaBuilder addJarveyColumn(int idx, String name, JarveyDataType jtype) {
		m_columns.add(idx, new JarveyColumn(m_columns.size(), name, jtype));
		if ( m_defaultGeomColName == null && jtype.isGeometryType() ) {
			m_defaultGeomColName = name;
		}
		
		return this;
	}
	
	public JarveySchemaBuilder addOrReplaceJarveyColumn(String name, JarveyDataType jtype) {
		CIString colName = CIString.of(name);
		int idx = Iterables.indexOf(m_columns, c -> c.getName().equals(colName));
		if ( idx < 0 ) {
			return addJarveyColumn(name, jtype);
		}
		else {
			m_columns.set(idx, new JarveyColumn(m_columns.size(), name, jtype));
			if ( colName.equals(m_defaultGeomColName) ) {
				if ( !jtype.isGeometryType() ) {
					m_defaultGeomColName = null;
				}
			}
		}
		
		return this;
	}
	
	public JarveySchemaBuilder addJarveyColumn(JarveyColumn jcol) {
		return addJarveyColumn(jcol.getName().get(), jcol.getJarveyDataType());
	}
	
	public JarveySchemaBuilder addEnvelopeColumn(String name) {
		return addJarveyColumn(name, EnvelopeType.get());
	}
	public JarveySchemaBuilder addGridCellColumn(String name) {
		return addJarveyColumn(name, GridCellType.get());
	}
	
	public JarveySchemaBuilder setDefaultGeometryColumn(String name) {
		m_defaultGeomColName = name;
		return this;
	}
	
	public JarveySchemaBuilder setQuadIds(long[] qids) {
		m_quadIds = qids;
		return this;
	}
}
