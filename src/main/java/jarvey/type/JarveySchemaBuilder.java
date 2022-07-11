/**
 * 
 */
package jarvey.type;

import java.util.List;

import org.apache.spark.sql.types.DataType;

import com.google.common.collect.Lists;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveySchemaBuilder {
	private List<JarveyColumn> m_columns = Lists.newArrayList();
	private String m_defaultGeomColName;
	private Long[] m_quadIds;
	
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
	
	public JarveySchemaBuilder addJarveyColumn(JarveyColumn jcol) {
		return addJarveyColumn(jcol.getName().get(), jcol.getJarveyDataType());
	}
	
	public JarveySchemaBuilder addRegularColumn(String name, DataType dataType) {
		return addJarveyColumn(name, RegularType.of(dataType));
	}
	
	public JarveySchemaBuilder addGeometryColumn(String name, int srid) {
		return addJarveyColumn(name, GeometryType.of(srid));
	}
	
	public JarveySchemaBuilder addEnvelopeColumn(String name) {
		return addJarveyColumn(name, EnvelopeType.get());
	}
	
	public JarveySchemaBuilder setDefaultGeometryColumn(String name) {
		m_defaultGeomColName = name;
		return this;
	}
	
	public JarveySchemaBuilder setQuadIds(Long[] qids) {
		m_quadIds = qids;
		return this;
	}
}
