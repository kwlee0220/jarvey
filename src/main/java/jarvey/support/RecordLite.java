package jarvey.support;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import com.google.common.base.Objects;

import jarvey.type.JarveySchema;

import utils.stream.IntFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordLite implements Comparable<RecordLite>, Serializable {
	private static final long serialVersionUID = -1409256329118309210L;
	
	protected Object[] m_values;
	
	public static RecordLite from(Row row) {
		return of(Rows.getValueList(row).toArray());
	}
	
	public static RecordLite from(Row row, int[] geomColIdxes) {
		Object[] values = Rows.getValueList(row).toArray();
		try {
			for ( int geomColIdx: geomColIdxes ) {
				values[geomColIdx] = GeoUtils.fromWKB((byte[])values[geomColIdx]);
			}
		}
		catch ( ParseException e ) {
			throw new RuntimeException(e);
		}
		
		return of(values);
	}
	
	public Row toRow(int[] geomColIdxes) {
		for ( int geomColIdx: geomColIdxes ) {
			m_values[geomColIdx] = GeoUtils.toWKB((Geometry)m_values[geomColIdx]);
		}
		
		return RowFactory.create(m_values);
	}
	
	public static RecordLite of(Object... values) {
		return new RecordLite(values);
	}
	
	public static RecordLite of(JarveySchema jschema) {
		return new RecordLite(new Object[jschema.getColumnCount()]);
	}
	
	public static RecordLite ofLength(int length) {
		return new RecordLite(new Object[length]);
	}
	
	protected RecordLite(Object[] values) {
		m_values = values;
	}
	
	public int length() {
		return m_values.length;
	}
	
	public Object get(int idx) {
		return m_values[idx];
	}
	
	public int getInteger(int idx) {
		return (Integer)m_values[idx];
	}
	
	public long getLong(int idx) {
		return (long)m_values[idx];
	}
	public Long[] getLongArray(int idx) {
		return (Long[])m_values[idx];
	}
	
	public float getFloat(int idx) {
		return (Float)m_values[idx];
	}
	
	public double getDouble(int idx) {
		return (Double)m_values[idx];
	}
	
	public String getString(int idx) {
		return (String)m_values[idx];
	}
	
	public Envelope getEnvelope(int idx) {
		return (Envelope)m_values[idx];
	}
	
	public Geometry getGeometry(int idx) {
		return (Geometry)m_values[idx];
	}
	
	public RecordLite select(int... colIdxes) {
		Object[] selecteds = new Object[colIdxes.length];
		for ( int i =0; i < colIdxes.length; ++i ) {
			selecteds[i] = m_values[colIdxes[i]];
		}
		
		return RecordLite.of(selecteds);
	}
	
	public void set(int idx, Object value) {
		m_values[idx] = value;
	}
	
	public void set(RecordLite rec) {
		setAll(rec.values());
	}
	
	public void setAll(int start, Object[] values, int offset, int length) {
		System.arraycopy(values, offset, m_values, start, length);
	}
	
	public void setAll(Object[] values) {
		System.arraycopy(values, 0, m_values, 0, values.length);
	}
	
	public void setAll(int start, List<Object> values, int offset, int length) {
		for ( int i = 0; i < length; ++i ) {
			int idx = i + offset;
			if ( idx >= values.size() ) {
				break;
			}
			
			m_values[start++] = values.get(idx);
		}
	}
	
	public RecordLite setAll(List<Object> values) {
		for ( int i =0; i < Math.min(values.size(), m_values.length); ++i ) {
			m_values[i] = values.get(i);
		}
		
		return this;
	}
	
	public Object getLast() {
		return m_values[m_values.length-1];
	}
	
	public Object[] values() {
		return m_values;
	}
	
	public void copyTo(RecordLite record, int srcPos, int destPos, int length) {
		System.arraycopy(m_values, srcPos, record.m_values, destPos, length);
	}
	
	public RecordLite duplicate() {
		Object[] values = new Object[m_values.length];
		System.arraycopy(m_values, 0, values, 0, m_values.length);
		return RecordLite.of(values);
	}
	
	public void copyTo(RecordLite record) {
		System.arraycopy(m_values, 0, record.m_values, 0, m_values.length);
	}
	
	public RecordLite copyOfRange(int start, int end) {
		return RecordLite.of(Arrays.copyOfRange(m_values, start, end));
	}
	
	public RecordLite project(int... indxes) {
		Object[] values = IntFStream.of(indxes).mapToObj(idx -> m_values[idx]).toArray(Object.class);
		return RecordLite.of(values);
	}
	
	public static RecordLite concat(RecordLite r1, RecordLite r2) {
		Object[] values = new Object[r1.values().length + r2.values().length];
		System.arraycopy(r1.values(), 0, values, 0, r1.values().length);
		System.arraycopy(r2.values(), 0, values, r1.values().length, r2.values().length);
		
		return RecordLite.of(values);
	}
	
	@Override
	public String toString() {
		return Arrays.toString(m_values);
	}

	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != RecordLite.class ) {
			return false;
		}
		
		RecordLite other = (RecordLite)obj;
		return Arrays.equals(m_values, other.m_values);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(m_values);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public int compareTo(RecordLite other) {
		for ( int i =0; i < m_values.length; ++i ) {
			int cmp = ((Comparable)m_values[i]).compareTo(other.m_values[i]);
			if ( cmp != 0 ) {
				return cmp;
			}
		}
		
		return 0;
	}
	
	private Object writeReplace() {
		throw new RuntimeException("should not be called");
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final Row m_row;
		
		private SerializationProxy(RecordLite part) {
			throw new RuntimeException("should not be called");
		}
		
		private Object readResolve() {
			throw new RuntimeException("should not be called");
		}
	}
}
