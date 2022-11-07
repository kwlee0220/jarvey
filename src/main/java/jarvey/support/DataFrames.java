package jarvey.support;

import static org.apache.spark.ml.functions.array_to_vector;
import static org.apache.spark.ml.functions.vector_to_array;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.when;

import java.util.Set;

import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Sets;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataFrames {
	private DataFrames() {
		throw new AssertionError("should not be called: " + getClass());
	}
	
	public static Dataset<Row> flattenStruct(Dataset<Row> rows, String[] targetCols,
											boolean keepSingleFieldStructName) {
		Set<String> targets = Sets.newHashSet(targetCols);
		Column[] colExprs = FStream.of(rows.schema().fields())
									.flatMap(field -> {
										String fieldName = field.name();
										if ( targets.contains(fieldName)
											&& field.dataType() instanceof StructType ) {
											return flattenStructField(rows, field, keepSingleFieldStructName);
										}
										else {
											return FStream.of(rows.col(fieldName));
										}
									})
									.toArray(Column.class);
		return rows.select(colExprs);
	}
	
	public static Dataset<Row> flattenStruct(Dataset<Row> rows, boolean keepSingleFieldStructName) {
		Column[] colExprs = FStream.of(rows.schema().fields())
									.flatMap(field -> {
										if ( field.dataType() instanceof StructType ) {
											return flattenStructField(rows, field, keepSingleFieldStructName);
										}
										else {
											return FStream.of(rows.col(field.name()));
										}
									})
									.toArray(Column.class);
		return rows.select(colExprs);
	}
	private static FStream<Column> flattenStructField(Dataset<Row> rows, StructField field,
													boolean keepSingleFieldStructName) {
		StructType type = (StructType)field.dataType();
		String parent = field.name();
		StructField[] childFields = type.fields();
		
		if ( childFields.length == 1 ) {
			String name = childFields[0].name();
			return FStream.of(rows.col(String.format("%s.%s", parent, name)).as(parent));
		}
		else {
			return FStream.of(childFields)
							.map(StructField::name)
							.map(name -> rows.col(String.format("%s.%s", parent, name))
												.as(String.format("%s_%s", parent, name)));
		}
	}
	
	public static Dataset<Row> toVector(Dataset<Row> rows, String[] cols, String outCol) {
		VectorAssembler assembler = new VectorAssembler()
										.setInputCols(cols)
										.setOutputCol(outCol);
		return assembler.transform(rows);
	}
	
	public static Dataset<Row> toVector(Dataset<Row> rows, String inCol, String outCol) {
		DataType colType = rows.schema().apply(inCol).dataType();
		if ( colType instanceof VectorUDT ) {
			rows = rows.withColumn(outCol, rows.col(inCol));
		}
		else if ( colType instanceof ArrayType ) {
			rows = rows.withColumn(outCol, array_to_vector(rows.col(inCol)));
		}
		else if ( colType == DataTypes.DoubleType || colType == DataTypes.FloatType ) {
			rows = toArray(rows, new String[]{inCol}, inCol + "_array");
			rows = rows.withColumn(outCol, array_to_vector(rows.col(inCol + "_array")));
		}
		else {
			throw new IllegalArgumentException("input column is not array type: " + inCol);
		}
		
		return rows;
	}
	
	public static Dataset<Row> toDoubleArray(Dataset<Row> rows, String vectorCol, String outCols) {
		DataType colType = rows.schema().apply(vectorCol).dataType();
		if ( colType instanceof VectorUDT ) {
			return rows.withColumn(outCols, vector_to_array(rows.col(vectorCol), "float64"));
		}
		else {
			throw new IllegalArgumentException("input column is not vector type: " + vectorCol);
		}
	}
	
	public static Dataset<Row> toFloatArray(Dataset<Row> rows, String vectorCol, String outCols) {
		DataType colType = rows.schema().apply(vectorCol).dataType();
		if ( !(colType instanceof VectorUDT) ) {
			throw new IllegalArgumentException("input column is not vector type: " + vectorCol);
		}
		
		return rows.withColumn(outCols, vector_to_array(rows.col(vectorCol), "float32"));
	}
	
	public static Dataset<Row> toArray(Dataset<Row> rows, String[] cols, String outCol) {
		Column[] colExprs = FStream.of(cols).map(rows::col).toArray(Column.class);
		return rows.withColumn(outCol, array(colExprs));
	}
	
	public static Dataset<Row> toMultiColumns(Dataset<Row> rows, String arrayCol, String[] outCols) {
		DataType colType = rows.schema().apply(arrayCol).dataType();
		if ( colType instanceof VectorUDT ) {
			rows = rows.withColumn(arrayCol, vector_to_array(rows.col(arrayCol), "float64"));
		}
		else if ( !(colType instanceof ArrayType) ) {
			throw new IllegalArgumentException("input column is not array type: " + arrayCol);
		}
		
		for ( int i =0; i < outCols.length; ++i ) {
			Column arrColExpr = rows.col(arrayCol);
			rows = rows.withColumn(outCols[i],
									when(lit(i).lt(size(arrColExpr)), element_at(arrColExpr, i+1))
									.otherwise(null));
		}
		return rows;
	}
	
	private static Dataset<Row> toFeature(Dataset<Row> rows, String inputCol) {
		DataType inColType = rows.schema().apply(inputCol).dataType();
		if ( inColType instanceof VectorUDT ) { }
		else if ( inColType instanceof ArrayType ) {
			DataType elmType = ((ArrayType)inColType).elementType();
			if ( elmType == DataTypes.DoubleType || elmType == DataTypes.FloatType ) {
				rows = toVector(rows, inputCol, inputCol + "_feature");
			}
			else {
				throw new IllegalArgumentException("invalid array element type: " + elmType);
			}
		}
		else if ( inColType == DataTypes.DoubleType || inColType == DataTypes.FloatType ) {
			rows = toArray(rows, new String[]{inputCol}, inputCol + "_array");
			rows = toVector(rows, inputCol + "_array", inputCol + "_feature");
		}
		else {
			throw new IllegalArgumentException("invalid input column type: " + inColType);
		}
		
		return rows;
	}
	
	public static Dataset<Row> scaleMinMax(Dataset<Row> rows, String inputCol, String outCol,
											double min, double max) {
		DataType inColType = rows.schema().apply(inputCol).dataType();
		if ( !(inColType instanceof VectorUDT) ) {
			rows = toVector(rows, inputCol, inputCol + "_feature");
		}
		
		rows = new MinMaxScaler().setMin(min).setMax(max)
					.setInputCol(inputCol + "_feature").setOutputCol(outCol + "_feature")
					.fit(rows)
					.transform(rows);
		
		if ( inColType instanceof VectorUDT ) {
			rows = rows.withColumnRenamed(outCol + "_feature", outCol);
		}
		else if ( inColType instanceof ArrayType ) {
			DataType elmType = ((ArrayType)inColType).elementType();
			if ( elmType == DataTypes.DoubleType ) {
				rows = toDoubleArray(rows, outCol + "_feature", outCol);
			}
			else if ( elmType == DataTypes.FloatType ) {
				rows = toFloatArray(rows, outCol + "_feature", outCol);
			}
			else {
				throw new IllegalArgumentException("invalid array element type: " + elmType);
			}
			rows = rows.drop(inputCol + "_feature", outCol + "_feature");
		}
		else if ( inColType == DataTypes.DoubleType || inColType == DataTypes.FloatType ) {
			rows = ( inColType == DataTypes.DoubleType )
					? toDoubleArray(rows, outCol + "_feature", outCol + "_array")
					: toFloatArray(rows, outCol + "_feature", outCol + "_array");
			rows = toMultiColumns(rows, outCol + "_array", new String[]{outCol});
			rows = rows.drop(inputCol + "_feature", inputCol + "_array",
								outCol + "_feature", outCol + "_array");
		}
		else {
			throw new IllegalArgumentException("invalid input column type: " + inColType);
		}
		
		return rows;
	}
}
