package jarvey.type;

import java.util.Objects;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import jarvey.datasource.DatasetException;

import scala.collection.mutable.WrappedArray;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class GridCellType extends JarveyDataType {
	private static final long serialVersionUID = 1L;
	private static final DataType SPARK_TYPE = DataTypes.createArrayType(DataTypes.IntegerType);
	
	private static final GridCellType SINGLETON = new GridCellType();
	public static final GridCellType get() {
		return SINGLETON;
	}
	
	protected GridCellType() {
		super(SPARK_TYPE);
	}

	@Override
	public Class<?> getJavaClass() {
		return GridCell.class;
	}
	
	@Override
	public  Integer[] serialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof GridCell ) {
			GridCell cell = (GridCell)value;
			return new Integer[]{ cell.getX(), cell.getY() };
		}
		else {
			throw new DatasetException("invalid GridCell: " + value);
		}
	}

	@Override
	public GridCell deserialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof WrappedArray ) {
			@SuppressWarnings("unchecked")
			WrappedArray<Integer> wrapped = (WrappedArray<Integer>)value;
			return newInstance(wrapped.apply(0), wrapped.apply(1));
		}
		else if ( value.getClass() == Integer[].class ) {
			Integer[] wrapped = (Integer[])value;
			return newInstance(wrapped[0], wrapped[1]);
		}
		else if ( value.getClass() == int[].class ) {
			int[] wrapped = (int[])value;
			return newInstance(wrapped[0], wrapped[1]);
		}
		else {
			throw new DatasetException("invalid serialized value for GridCell: " + value);
		}
	}
	
	@Override
	public String toString() {
		return "GridCell";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof GridCellType) ) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(getSparkType());
	}
	
	private static GridCell newInstance(int x, int y) {
		return new GridCell(x, y);
	}
}
