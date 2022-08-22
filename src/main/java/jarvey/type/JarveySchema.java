package jarvey.type;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Geometry;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.Yaml;

import avro.shaded.com.google.common.collect.Maps;
import jarvey.support.SchemaUtils;
import scala.collection.Iterator;
import utils.CIString;
import utils.func.FOption;
import utils.func.KeyValue;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveySchema implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final StructType m_schema;
	private final Map<CIString,JarveyColumn> m_colInfos;
	private final List<JarveyColumn> m_columns;
	@Nullable private final JarveyColumn m_defaultGeometryColumn;
	@Nullable private final Long[] m_quadIds;
	
	JarveySchema(List<JarveyColumn> columns, @Nullable String defGeomColName,
						@Nullable Long[] quadIds) {
		m_schema = DataTypes.createStructType(FStream.from(columns)
													.map(JarveyColumn::toStructField)
													.toList());
		m_colInfos = FStream.from(columns)
							.mapToKeyValue(jcol -> KeyValue.of(jcol.getName(), jcol))
							.toMap();
		m_columns = columns;
		m_quadIds = quadIds;
		m_defaultGeometryColumn = (defGeomColName != null) ? m_colInfos.get(CIString.of(defGeomColName)) : null;
	}
	
	public StructType getSchema() {
		return m_schema;
	}
	
	public List<JarveyColumn> getColumnAll() {
		return m_columns;
	}
	
	public FOption<JarveyColumn> findColumn(String name) {
		return FOption.ofNullable(m_colInfos.get(CIString.of(name)));
	}
	
	public JarveyColumn getColumn(String name) {
		JarveyColumn info = m_colInfos.get(CIString.of(name));
		if ( info == null ) {
			throw new IllegalArgumentException("invalid column: " + name);
		}
		
		return info;
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
			return new GeometryColumnInfo(m_defaultGeometryColumn.getName().get(),
											((GeometryType)jtype).getSrid());
		}
		else {
			return null;
		}
	}
	
	public @Nullable JarveyColumn getDefaultGeometryColumn() {
		return m_defaultGeometryColumn;
	}
	
	public JarveySchema setDefaultGeometryColumn(String colName) {
		JarveyDataType jtype = getColumn(colName).getJarveyDataType();
		if ( jtype instanceof GeometryType ) {
			throw new IllegalArgumentException("invalid column: not geometry column=" + colName);
		}
		
		return new JarveySchema(m_columns, colName, m_quadIds);
	}
	
	public Geometry getDefaultGeometry(Row row) {
		return (m_defaultGeometryColumn != null)
					? GeometryValue.deserialize(row.getAs(m_defaultGeometryColumn.getIndex()))
					: null;
	}
	
	public Long[] getQuadIds() {
		return m_quadIds;
	}
	
	public JarveySchema select(String... cols) {
		List<JarveyColumn> columns = FStream.of(cols).map(this::getColumn).toList();
		Set<CIString> targets = FStream.of(cols).map(CIString::of).toSet();
		if ( targets.contains(m_defaultGeometryColumn.getName()) ) {
			return new JarveySchema(columns, m_defaultGeometryColumn.getName().get(), m_quadIds);
		}
		else {
			return new JarveySchema(columns, null, m_quadIds);
		}
	}
	
	public JarveySchema rename(String oldColName, String newColName) {
		List<JarveyColumn> columns
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
									.toList();
		if ( m_defaultGeometryColumn.getName().equals(oldColName) ) {
			return new JarveySchema(columns, newColName, m_quadIds);
		}
		else {
			return new JarveySchema(columns, m_defaultGeometryColumn.getName().get(), m_quadIds);
		}
	}
	
	/**
	 * remove columns of the given names.
	 * 
	 * @param cols	column names to be removed
	 * @return		a new JarveySchema after the target columns are removed.
	 */
	public JarveySchema drop(String... cols) {
		Set<CIString> targets = FStream.of(cols).map(CIString::of).toSet();
		
		List<JarveyColumn> columns = FStream.from(m_columns)
											.filter(c -> !targets.contains(c.getName()))
											.zipWithIndex()
											.map(t -> new JarveyColumn(t._2, t._1.getName(), t._1.getJarveyDataType()))
											.toList();
		String defGeomCol = (targets.contains(m_defaultGeometryColumn.getName()))
							? null : m_defaultGeometryColumn.getName().get();
		return new JarveySchema(columns, defGeomCol, m_quadIds);
	}
	
	public JarveySchemaBuilder toBuilder() {
		JarveySchemaBuilder builder = FStream.from(m_columns)
											.foldLeft(new JarveySchemaBuilder(),
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
						.foldLeft(builder(), (b, c) -> b.addJarveyColumn(c))
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
	
	public static JarveySchema fromStructType(StructType structType) {
		JarveySchemaBuilder builder = JarveySchema.builder();
		Iterator<StructField> it = structType.iterator();
		while ( it.hasNext() ) {
			StructField field = it.next();
			builder = builder.addRegularColumn(field.name(), field.dataType());
		}
		
		return builder.build();
	}
	
	public JarveySchema updateRegularColumn(StructType refSchema, String colName) {
		int idx = findColumn(colName).map(JarveyColumn::getIndex).getOrElse(-1);
		if ( idx >= 0 ) {
			StructField field = refSchema.fields()[idx];
			return update(colName, info -> new JarveyColumn(-1, colName, RegularType.of(field.dataType())));
		}
		else {
			StructField last = SchemaUtils.last(refSchema);
			return toBuilder().addRegularColumn(last.name(), last.dataType()).build();
		}
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
		List<JarveyColumn> columns = FStream.from(infosYaml)
											.map(colYaml -> JarveyColumn.fromYaml(-1, colYaml))
											.toList();
		String defGeomCol = FOption.ofNullable((String)yaml.get("default_geometry")).getOrNull();
		List<Object> qidList = (List<Object>)yaml.get("quad_ids");
		Long[] qids = null;
		if ( qidList != null ) {
			qids = FStream.from(qidList).map(DataUtils::asLong).toArray(Long.class);
		}
		return new JarveySchema(columns, defGeomCol, qids);
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
	
	public static JarveySchema fromYaml(Map<String,Object> yaml) {
		List<Map<String,Object>> columnYamls = (List<Map<String,Object>>)yaml.get("columns");
		List<JarveyColumn> columns = FStream.from(columnYamls)
											.zipWithIndex()
											.map(t -> JarveyColumn.fromYaml(t._2, t._1))
											.toList();
		String defGeomCol = (String)yaml.get("default_geometry");
		Long[] quids = (Long[])yaml.get("quad_ids");
		
		return new JarveySchema(columns, defGeomCol, quids);
	}
	
	@Override
	public String toString() {
		String gcInfoStr = (m_defaultGeometryColumn != null) ? m_defaultGeometryColumn.getName().get() : "?";
		String colNames = FStream.from(m_columns).map(c -> c.getName().get()).join(',');
		String clusterStr = (m_quadIds != null) ? ", nclusters=" + m_quadIds.length : "";
		
		return String.format("[%s][%d] %s%s", gcInfoStr, m_colInfos.size(), colNames, clusterStr);
	}
}
