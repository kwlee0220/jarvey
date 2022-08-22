package jarvey.support;

import java.util.List;
import java.util.function.Function;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;

import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class SchemaUtils {
	private SchemaUtils() {
		throw new AssertionError("Should not be called here: class=" + getClass());
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
	
	public static int indexOf(StructType schema, String name) {
		StructField[] fields = schema.fields();
		for ( int i =0; i < fields.length; ++i ) {
			if ( fields[i].name().equalsIgnoreCase(name) ) {
				return i;
			}
		}
		return -1;
	}
	
	public static StructField last(StructType schema) {
		StructField[] fields = schema.fields();
		return fields[fields.length-1];
	}
}
