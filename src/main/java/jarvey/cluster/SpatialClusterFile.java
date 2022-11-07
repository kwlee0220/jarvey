package jarvey.cluster;

import java.io.IOException;
import java.util.TreeSet;

import com.google.common.collect.Sets;

import jarvey.FilePath;
import jarvey.JarveySession;

import utils.func.UncheckedPredicate;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialClusterFile {
	private final JarveySession m_jarvey;
	private final FilePath m_path;
	private final TreeSet<SpatialPartitionFile> m_partitions;
	
	public static SpatialClusterFile of(JarveySession jarvey, FilePath path) throws IOException {
		return new SpatialClusterFile(jarvey, path);
	}
	
	public static SpatialClusterFile fromDataset(JarveySession jarvey, String dsId) throws IOException {
		return SpatialClusterFile.of(jarvey, jarvey.getClusterFilePath(dsId));
	}
	
	SpatialClusterFile(JarveySession jarvey, FilePath path) throws IOException {
		m_jarvey = jarvey;
		m_path = path;
		
		m_partitions = path.streamChildFilePaths()
							.filter(UncheckedPredicate.sneakyThrow(SpatialPartitionFile::isPartitionFile))
							.mapOrThrow(p -> SpatialPartitionFile.fromFilePath(jarvey, p))
							.collectLeft(Sets.newTreeSet(), (acc,p) -> acc.add(p));
	}
	
	public FilePath getFilePath() {
		return m_path;
	}
	
	public int getPartitionCount() {
		return m_partitions.size();
	}
	
	public Long[] toQuadIds() {
		return FStream.from(m_partitions).map(SpatialPartitionFile::getQuadId).toArray(Long.class);
	}
	
	public SpatialPartitionFile getPartitionFile(long quadId) {
		return FStream.from(m_partitions).findFirst(spf -> spf.getQuadId() == quadId).getOrNull();
	}
	
	public FStream<SpatialPartitionFile> streamPartitions(boolean excludeOutlierFile) throws IOException {
		return streamPartitionsBySize(false, excludeOutlierFile);
	}
	
	public FStream<SpatialPartitionFile> streamPartitionsBySize(boolean reverse, boolean excludeOutlierFile)
		throws IOException {
		FStream<SpatialPartitionFile> strm = reverse ? FStream.from(m_partitions.descendingIterator())
														: FStream.from(m_partitions);
		if ( excludeOutlierFile ) {
			strm = strm.filterNot(SpatialPartitionFile::isOutlier);
		}
		
		return strm;
	}
}
