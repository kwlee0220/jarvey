package jarvey.support;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.Utilities;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class SchemaUtils {
	private SchemaUtils() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	public static SchemaBuilder toBuilder(StructType schema) {
		return FStream.of(schema.fields())
						.foldLeft(builder(), (b,f) -> b.addOrReplaceField(f.name(), f.dataType()));
	}
	
	public static SchemaBuilder builder() {
		return new SchemaBuilder();
	}
	public static class SchemaBuilder {
		private final Map<String,DataType> m_fields;
		
		private SchemaBuilder() {
			m_fields = Maps.newLinkedHashMap();
		}
		
		public SchemaBuilder addOrReplaceField(String name, DataType type) {
			m_fields.put(name, type);
			return this;
		}
		
		public SchemaBuilder addOrReplaceField(StructField field) {
			m_fields.put(field.name(), field.dataType());
			return this;
		}
		
		public StructType build() {
			List<StructField> fieldList = FStream.from(m_fields)
												.map(kv -> DataTypes.createStructField(kv.key(), kv.value(), true))
												.toList();
			return DataTypes.createStructType(fieldList);
		}
	}
	
	public static StructType update(StructType schema, String colName, Function<StructField,StructField> updater) {
		List<StructField> newFieldList = Lists.newArrayList();
		
		StructField[] oldFields = schema.fields();
		for ( int i =0; i < oldFields.length; ++i ) {
			StructField field = oldFields[i];
			if ( colName.equals(field.name()) ) {
				newFieldList.add(updater.apply(field));
			}
		}

		return DataTypes.createStructType(newFieldList);
	}
	
	public static StructField get(StructType schema, String name) {
		return FStream.of(schema.fields())
						.findFirst(field -> field.name().equalsIgnoreCase(name))
						.getOrNull();
	}
	
	public static int fieldIndex(StructType schema, String name) {
		StructField[] fields = schema.fields();
		for ( int i =0; i < fields.length; ++i ) {
			if ( fields[i].name().equalsIgnoreCase(name) ) {
				return i;
			}
		}
		return -1;
	}
	
	public static int[] fieldIndexes(StructType schema, String[] names) {
		int[] idxes = new int[names.length];
		for ( int i =0; i < names.length; ++i ) {
			idxes[i] = fieldIndex(schema, names[i]);
		}
		return idxes;
	}
	
	public static StructField last(StructType schema) {
		StructField[] fields = schema.fields();
		return fields[fields.length-1];
	}
	
	public static StructType subrange(StructType schema, int begin, int end) {
		return FStream.of(Arrays.copyOfRange(schema.fields(), begin, end))
		 				.foldLeft(builder(), SchemaBuilder::addOrReplaceField)
						.build();
	}

	public static StructType select(StructType schema, String[] colNames) {
		return select(schema, Arrays.asList(colNames));
	}
	public static StructType select(StructType schema, Iterable<String> colNames) {
		return FStream.from(colNames)
 						.map(n -> get(schema, n))
				 		.foldLeft(builder(), SchemaBuilder::addOrReplaceField)
				 		.build();
	}
	
	public static StructType complement(StructType schema, String... colNames) {
		return complement(schema, Arrays.asList(colNames));
	}
	public static StructType complement(StructType schema, Iterable<String> colNames) {
		Utilities.checkNotNullArgument(colNames, "column list is null");
		
		Set<String> names = Sets.newHashSet(colNames);
		return FStream.of(schema.fields())
						.filter(f -> !names.contains(f.name()))
						.foldLeft(builder(), SchemaBuilder::addOrReplaceField)
						.build();
	}
}
