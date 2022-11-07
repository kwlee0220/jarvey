package jarvey.datasource.shp;

import java.io.File;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.connector.read.InputPartition;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import utils.stream.FStream;

/**
*
* @author Kang-Woo Lee
*/
class MultiShapefilesPartition implements InputPartition {
	private static final long serialVersionUID = 1L;
	
	private final int m_index;
	private final File[] m_files;
	
	MultiShapefilesPartition(int index, List<File> files) {
		m_index = index;
		m_files = Iterables.toArray(files, File.class);
	}
	
	public int getIndex() {
		return m_index;
	}
	
	public File[] getFiles() {
		return m_files;
	}

	@Override
	public String[] preferredLocations() {
		return new String[0];
	}
	
	@Override
	public String toString() {
		return Arrays.toString(m_files);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != MultiShapefilesPartition.class ) {
			return false;
		}
		
		MultiShapefilesPartition other = (MultiShapefilesPartition)obj;
		return Objects.equal(m_index, other.m_index)
				&& Arrays.equals(m_files, other.m_files);
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final int m_index;
		private final String[] m_paths;
		
		private SerializationProxy(MultiShapefilesPartition part) {
			m_index = part.m_index;
			m_paths = FStream.of(part.m_files).map(File::getAbsolutePath).toArray(String.class);
		}
		
		private Object readResolve() {
			return new MultiShapefilesPartition(m_index, FStream.of(m_paths).map(File::new).toList());
		}
	}
}