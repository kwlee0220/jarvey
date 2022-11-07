package jarvey.support;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import jarvey.type.JarveyColumn;
import jarvey.type.JarveySchema;

import scala.collection.JavaConverters;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Rows {
	private Rows() {
		throw new AssertionError("should not be called: " + getClass());
	}
	
	public static StructType getSchema(Dataset<Row> rows) {
		return rows.exprEnc().schema();
	}
	
	public static List<Object> getValueList(Row row) {
		return JavaConverters.seqAsJavaListConverter(row.toSeq()).asJava();
	}
	
	public static Object[] toArray(Row row) {
		return toArray(row, row.length(), row.length());
	}
	
	public static Object[] toArray(Row row, int bufLen, int copyLen) {
		Object[] values = new Object[bufLen];
		copyLen = Math.min(row.length(), copyLen);
		for ( int i =0; i < copyLen; ++i ) {
			values[i] = row.get(i);
		}
		
		return values;
	}
	
	public static StructField[] toFields(StructType schema, String... fieldNames) {
		StructField[] fields = schema.fields();
		return FStream.of(fieldNames)
						.map(fname -> fields[schema.fieldIndex(fname)])
						.toArray(StructField.class);
	}
	
	public static int[] toFieldIndexes(StructType schema, String... fieldNames) {
		return FStream.of(fieldNames)
						.mapToInt(n -> schema.fieldIndex(n))
						.toArray();
	}
	
	public static Row toRow(Object[] values) {
		return RowFactory.create(values);
	}
	
	public static JavaRDD<Row> toRowRDD(JarveySchema jschema, JavaRDD<RecordLite> recs) {
		int[] geomColIdxes = FStream.from(jschema.getColumnAll())
									.filter(jcol -> jcol.getJarveyDataType().isGeometryType())
									.mapToInt(JarveyColumn::getIndex)
									.toArray();
		return recs.map(rec -> rec.toRow(geomColIdxes));
	}
	
	public static JavaRDD<RecordLite> toRecordLiteRDD(JarveySchema jschema, JavaRDD<Row> rows) {
		int[] geomColIdxes = FStream.from(jschema.getColumnAll())
									.filter(jcol -> jcol.getJarveyDataType().isGeometryType())
									.mapToInt(JarveyColumn::getIndex)
									.toArray();
		return rows.map(row -> RecordLite.from(row, geomColIdxes));
	}
	
	public static Column[] toColumns(Dataset<Row> rows,  String... colNames) {
		return FStream.of(colNames).map(rows::col).toArray(Column.class);
	}
	
	public static Column[] toColumns(Dataset<Row> rows, String[] colNames, String... colNames2) {
		return toColumns(rows, Utilities.concat(colNames, colNames2));
	}
}
