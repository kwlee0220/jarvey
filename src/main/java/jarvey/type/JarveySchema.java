package jarvey.type;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Geometry;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Maps;

import utils.CIString;
import utils.Utilities;
import utils.func.FOption;
import utils.func.KeyValue;
import utils.stream.FStream;

import jarvey.datasource.DatasetException;
import jarvey.support.SchemaUtils;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveySchema implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final StructType m_schema;
	private final Map<CIString,JarveyColumn> m_columnsMap;
	private final List<JarveyColumn> m_columns;
	@Nullable private final JarveyColumn m_defaultGeometryColumn;
	@Nullable private final long[] m_quadIds;
	
	JarveySchema(List<JarveyColumn> columns, @Nullable String defGeomColName, @Nullable long[] quadIds) {
		// kwlee
		// JarveyColumn의 index값을 확정한다.
		FStream.from(columns)
				.zipWithIndex()
				.forEach(tup -> tup._1.setIndex(tup._2));
		
		// JarveyColumn들로 부터 Spark StructType을 계산한다.
		m_schema = DataTypes.createStructType(FStream.from(columns)
													.map(JarveyColumn::toStructField)
													.toList());
		m_columnsMap = FStream.from(columns)
								.mapToKeyValue(jcol -> KeyValue.of(jcol.getName(), jcol))
								.toMap();
		m_columns = columns;
		m_quadIds = quadIds;
		m_defaultGeometryColumn = (defGeomColName != null) ? m_columnsMap.get(CIString.of(defGeomColName)) : null;
	}
	
	public StructType getSchema() {
		return m_schema;
	}
	
	public int getColumnCount() {
		return m_columns.size();
	}
	
	public List<JarveyColumn> getColumnAll() {
		return m_columns;
	}
	
	public FOption<JarveyColumn> findColumn(String name) {
		return FOption.ofNullable(m_columnsMap.get(CIString.of(name)));
	}
	
	public JarveyColumn getColumn(String name) {
		JarveyColumn jcol = m_columnsMap.get(CIString.of(name));
		if ( jcol == null ) {
			throw new IllegalArgumentException("invalid column: " + name);
		}
		
		return jcol;
	}
	
	public JarveyColumn getColumn(int index) {
		if ( index >= 0 && index < m_columns.size() ) {
			return m_columns.get(index);
		}
		else {
			throw new IllegalArgumentException("invalid column index: " + index);
		}
	}
	
	public int getColumnIndex(String name) {
		return getColumn(name).getIndex();
	}
	
	public Object getValue(Row row, String colName) {
		JarveyColumn jcol = getColumn(colName);
		return jcol.getJarveyDataType().deserialize(row.getAs(jcol.getIndex()));
	}
	
	public Object getValue(Row row, int colIndex) {
		JarveyColumn jcol = getColumn(colIndex);
		return jcol.getJarveyDataType().deserialize(row.getAs(colIndex));
	}
	
	public List<Object> serializeToInternal(List<Object> colValues) {
		return FStream.from(getColumnAll())
						.map(JarveyColumn::getJarveyDataType)
						.zipWith(FStream.from(colValues))
						.map(t -> t._1.serializeToInternal(t._2))
						.toList();
	}
	
	public List<Object> serialize(List<Object> colValues) {
		return FStream.from(getColumnAll())
						.map(JarveyColumn::getJarveyDataType)
						.zipWith(FStream.from(colValues))
						.map(t -> t._1.serialize(t._2))
						.toList();
	}
	
	public List<Object> deserialize(Row row) {
		return FStream.from(getColumnAll())
						.map(JarveyColumn::getJarveyDataType)
						.zipWithIndex()
						.map(t -> t._1.deserialize(row.get(t._2)))
						.toList();
	}
	
	public @Nullable GeometryColumnInfo getDefaultGeometryColumnInfo() {
		if ( m_defaultGeometryColumn != null ) {
			GeometryType jtype = m_defaultGeometryColumn.getJarveyDataType().asGeometryType();
			return new GeometryColumnInfo(m_defaultGeometryColumn.getName().get(), jtype);
		}
		else {
			return null;
		}
	}
	
	public GeometryColumnInfo assertDefaultGeometryColumnInfo() {
		GeometryColumnInfo info = getDefaultGeometryColumnInfo();
		if ( info == null ) {
			throw new IllegalStateException("Default Geometry column is not found");
		}
		
		return info;
	}
	
	public @Nullable JarveyColumn getDefaultGeometryColumn() {
		return m_defaultGeometryColumn;
	}
	
	public JarveyColumn assertDefaultGeometryColumn() {
		JarveyColumn jcol = getDefaultGeometryColumn();
		if ( jcol == null ) {
			throw new IllegalStateException("Default Geometry column is not found");
		}
		
		return jcol;
	}
	
	public JarveySchema setDefaultGeometryColumn(String colName) {
		JarveyDataType jtype = getColumn(colName).getJarveyDataType();
		if ( !(jtype instanceof GeometryType) ) {
			throw new IllegalArgumentException("invalid column: not geometry column=" + colName);
		}
		
		return toBuilder().setDefaultGeometryColumn(colName)
							.build();
	}
	
	public Geometry getDefaultGeometry(Row row) {
		return (m_defaultGeometryColumn != null)
					? GeometryBean.deserialize(row.getAs(m_defaultGeometryColumn.getIndex()))
					: null;
	}
	
	public int getSrid() {
		return assertDefaultGeometryColumnInfo().getSrid();
	}
	
	public long[] getQuadIds() {
		return m_quadIds;
	}
	
	public JarveySchema select(String... cols) {
		 JarveySchemaBuilder builder
		 		= FStream.of(cols)
 						.map(this::getColumn)
				 		.fold(JarveySchema.builder(), JarveySchemaBuilder::addJarveyColumn);
		 builder = builder.setQuadIds(m_quadIds);
		
		Set<CIString> targets = FStream.of(cols).map(CIString::of).toSet();
		if ( m_defaultGeometryColumn != null
			&& targets.contains(m_defaultGeometryColumn.getName()) ) {
			builder = builder.setDefaultGeometryColumn(m_defaultGeometryColumn.getName().get());
		}
		return builder.build();
	}
	
	public JarveySchema rename(String oldColName, String newColName) {
		JarveySchemaBuilder builder
				= FStream.from(m_columns)
						.map(jcol -> {
							if ( jcol.getName().equals(oldColName) ) {
								return new JarveyColumn(jcol.getIndex(), CIString.of(newColName),
														jcol.getJarveyDataType());
							}
							else {
								return jcol;
							}
						})
						.fold(JarveySchema.builder(), (b,c) -> b.addJarveyColumn(c));
		if ( m_defaultGeometryColumn != null && m_defaultGeometryColumn.getName().equals(oldColName) ) {
			builder = builder.setDefaultGeometryColumn(newColName)
							.setQuadIds(m_quadIds);
		}
		return builder.build();
	}
	
	public static JarveySchema concat(JarveySchema schema1, JarveySchema schema2) {
		return FStream.from(schema2.getColumnAll())
						.fold(schema1.toBuilder(), JarveySchemaBuilder::addJarveyColumn)
						.build();
	}
	
	/**
	 * remove columns of the given names.
	 * 
	 * @param cols	column names to be removed
	 * @return		a new JarveySchema after the target columns are removed.
	 */
	public JarveySchema drop(String... cols) {
		Set<CIString> targets = FStream.of(cols).map(CIString::of).toSet();
		
		JarveySchemaBuilder builder = FStream.from(m_columns)
											.filter(c -> !targets.contains(c.getName()))
											.fold(JarveySchema.builder(),
													JarveySchemaBuilder::addJarveyColumn);
		if ( m_defaultGeometryColumn != null ) {
			String defGeomCol = (targets.contains(m_defaultGeometryColumn.getName()))
								? null : m_defaultGeometryColumn.getName().get();
			builder = builder.setDefaultGeometryColumn(defGeomCol);
			builder = builder.setQuadIds(m_quadIds);
		}
		return builder.build();
	}
	
	public JarveySchemaBuilder toBuilder() {
		JarveySchemaBuilder builder = FStream.from(m_columns)
											.fold(new JarveySchemaBuilder(),
														(acc, jcol) -> acc.addJarveyColumn(jcol));
		if ( m_defaultGeometryColumn != null ) {
			builder.setDefaultGeometryColumn(m_defaultGeometryColumn.getName().get());
		}
		builder.setQuadIds(m_quadIds);
		
		return builder;
	}
	
	public static JarveySchemaBuilder builder() {
		return new JarveySchemaBuilder();
	}
	
	public JarveySchema update(String colName, Function<JarveyColumn,JarveyColumn> updater) {
		return FStream.from(m_columns)
						.map(jcol -> (jcol.getName().equals(colName)) ? updater.apply(jcol) : jcol)
						.fold(builder(), (b, c) -> b.addJarveyColumn(c))
						.build();
	}
	
	public JarveySchema updateColumn(StructType refSchema, String colName, JarveyDataType jtype) {
		int idx = findColumn(colName).map(JarveyColumn::getIndex).getOrElse(-1);
		if ( idx >= 0 ) {
			return update(colName, info -> new JarveyColumn(-1, colName, jtype));
		}
		else {
			StructField last = SchemaUtils.last(refSchema);
			return toBuilder().addJarveyColumn(last.name(), jtype).build();
		}
	}
	
	public JarveySchema updateRegularColumn(StructType refSchema, String colName) {
		int idx = findColumn(colName).map(JarveyColumn::getIndex).getOrElse(-1);
		if ( idx >= 0 ) {
			StructField field = refSchema.fields()[idx];
			JarveyDataType jtype = JarveyDataTypes.fromSparkType(field.dataType());
			return update(colName, info -> new JarveyColumn(-1, colName, jtype));
		}
		else {
			StructField last = SchemaUtils.last(refSchema);
			JarveyDataType jtype = JarveyDataTypes.fromSparkType(last.dataType());
			return toBuilder().addJarveyColumn(last.name(), jtype).build();
		}
	}
	
	public JarveySchema complement(Iterable<String> key) {
		Utilities.checkNotNullArgument(key, "key column list is null");
		
		Set<CIString> names = FStream.from(key).map(CIString::of).toSet();
		return FStream.from(m_columns)
						.filter(jc -> !names.contains(jc.getName()))
						.fold(JarveySchema.builder(), (b,c) -> b.addJarveyColumn(c))
						.build();
	}
	
	public static JarveySchema readYaml(InputStream is) throws IOException {
		return readYaml(new InputStreamReader(is));
	}
	public static JarveySchema readYaml(Reader reader) throws IOException {
		Yaml parser = new Yaml();
		Map<String,Object> yaml = parser.load(reader);
		return JarveySchema.parseJarveySchema(yaml);
	}
	private static JarveySchema parseJarveySchema(Map<String,Object> yaml) {
		List<Map<String,Object>> infosYaml = (List<Map<String,Object>>)yaml.get("columns");
		JarveySchemaBuilder builder =  FStream.from(infosYaml)
											.map(colYaml -> JarveyColumn.fromYaml(-1, colYaml))
											.fold(JarveySchema.builder(),
														JarveySchemaBuilder::addJarveyColumn);
		builder = builder.setDefaultGeometryColumn(FOption.ofNullable((String)yaml.get("default_geometry")).getOrNull());
		List<Object> qidList = (List<Object>)yaml.get("quad_ids");
		if ( qidList != null ) {
			builder = builder.setQuadIds(FStream.from(qidList).mapToLong(DataUtils::asLong).toArray());
		}
		return builder.build();
	}
	
	public void writeAsYaml(Writer writer) throws IOException {
		PrintWriter pw = new PrintWriter(writer);
		DumperOptions opts = new DumperOptions();
		opts.setIndent(2);
		opts.setPrettyFlow(true);
		opts.setDefaultFlowStyle(FlowStyle.BLOCK);
		
		Yaml yaml = new Yaml(opts);
		yaml.dump(toYaml(), pw);
	}
	public void writeAsYaml(OutputStream os) throws IOException {
		writeAsYaml(new OutputStreamWriter(os));
	}
	
	private Map<String,Object> toYaml() {
		Map<String,Object> yaml = Maps.newLinkedHashMap();

		if ( m_defaultGeometryColumn != null ) {
			yaml.put("default_geometry", m_defaultGeometryColumn.getName().get());
		}
		List<Map<String,Object>> columnYamls = FStream.from(m_columns)
													.map(jcol -> jcol.toYaml())
													.toList();
		yaml.put("columns", columnYamls);
		
		if ( m_quadIds != null ) {
			yaml.put("quad_ids", m_quadIds);
		}
		
		return yaml;
	}
	
	public static JarveySchema fromStructType(StructType schema, @Nullable GeometryColumnInfo gcInfo) {
		JarveySchemaBuilder builder = JarveySchema.builder();
		for ( StructField field: schema.fields() ) {
			if ( gcInfo == null ) {
				JarveyDataType jtype = JarveyDataTypes.fromSparkType(field.dataType());
				builder = builder.addJarveyColumn(field.name(), jtype);
			}
			else if ( field.name().equalsIgnoreCase(gcInfo.getName()) ) {
				if ( field.dataType() instanceof BinaryType ) {
					builder = builder.addJarveyColumn(gcInfo.getName(), gcInfo.getDataType());
					builder = builder.setDefaultGeometryColumn(gcInfo.getName());
				}
				else {
					throw new DatasetException("invalid Spark type for Geometry: " + field.dataType());
				}
			}
			else {
				JarveyDataType jtype = JarveyDataTypes.fromSparkType(field.dataType());
				builder = builder.addJarveyColumn(field.name(), jtype);
			}
		}
		return builder.build();
	}
	
	public static JarveySchema fromYaml(Map<String,Object> yaml) {
		List<Map<String,Object>> columnYamls = (List<Map<String,Object>>)yaml.get("columns");
		JarveySchemaBuilder builder = FStream.from(columnYamls)
											.zipWithIndex()
											.map(t -> JarveyColumn.fromYaml(t._2, t._1))
											.fold(JarveySchema.builder(), JarveySchemaBuilder::addJarveyColumn);
		builder = builder.setDefaultGeometryColumn((String)yaml.get("default_geometry"));
		builder = builder.setQuadIds(FStream.of((Long[])yaml.get("quad_ids")).mapToLong(v -> v).toArray());
		return builder.build();
	}
	
	@Override
	public String toString() {
		String gcInfoStr = (m_defaultGeometryColumn != null) ? m_defaultGeometryColumn.getName().get() : "?";
		String colNames = FStream.from(m_columns).map(c -> c.getName().get()).join(',');
		String clusterStr = (m_quadIds != null) ? ", nclusters=" + m_quadIds.length : "";
		
		return String.format("[%s][%d] %s%s", gcInfoStr, m_columnsMap.size(), colNames, clusterStr);
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final List<JarveyColumn> m_columns;
		@Nullable private final String m_defGeomColName;
		@Nullable private final long[] m_quadIds;
		
		private SerializationProxy(JarveySchema jschema) {
			m_columns = jschema.m_columns;
			JarveyColumn geomCol = jschema.m_defaultGeometryColumn;
			m_defGeomColName = (geomCol != null) ? geomCol.getName().get() : null;
			m_quadIds = jschema.m_quadIds;
		}
		
		private Object readResolve() {
			return FStream.from(m_columns)
							.fold(JarveySchema.builder(), JarveySchemaBuilder::addJarveyColumn)
							.setDefaultGeometryColumn(m_defGeomColName)
							.setQuadIds(m_quadIds)
							.build();
		}
	}
}
