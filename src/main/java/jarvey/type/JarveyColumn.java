package jarvey.type;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import avro.shaded.com.google.common.collect.Maps;
import utils.CIString;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class JarveyColumn implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private int m_index;
	private final CIString m_name;
	private final JarveyDataType m_type;
	
	JarveyColumn(int index, String name, JarveyDataType jtype) {
		m_index = index;
		m_name = CIString.of(name);
		m_type = jtype;
	}
	
	JarveyColumn(int index, CIString name, JarveyDataType jtype) {
		m_index = index;
		m_name = name;
		m_type = jtype;
	}
	
	public int getIndex() {
		return m_index;
	}
	
	void setIndex(int index) {
		m_index = index;
	}
	
	public CIString getName() {
		return m_name;
	}

	public JarveyDataType getJarveyDataType() {
		return m_type;
	}
	
	public StructField toStructField() {
		return DataTypes.createStructField(m_name.get(), m_type.getSparkType(), true);
	}
	
	static JarveyColumn fromYaml(int index, Map<String,Object> yaml) {
		String name = (String)yaml.get("name");
		JarveyDataType jtype = JarveyDataType.fromString((String)yaml.get("type"));
		return new JarveyColumn(index, CIString.of(name), jtype);
	}
	
	Map<String,Object> toYaml() {
		Map<String,Object> yaml = Maps.newLinkedHashMap();
		yaml.put("name", m_name.get());
		yaml.put("type", m_type.toString());
		yaml.put("nullable", true);
		
		return yaml;
	}
	
	@Override
	public String toString() {
		return String.format("%s %s", m_name, m_type);
	}
}