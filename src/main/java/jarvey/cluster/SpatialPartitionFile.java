package jarvey.cluster;

import static org.apache.spark.sql.functions.lit;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Objects;

import jarvey.FilePath;
import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.datasource.JarveyDataFrameReader;
import jarvey.support.MapTile;
import jarvey.type.DataUtils;
import jarvey.type.JarveySchema;

import utils.UnitUtils;
import utils.func.Lazy;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialPartitionFile implements Comparable<SpatialPartitionFile> {
	private static final String OUTLIER_FILE_NAME = String.format("%s=-9", SpatialDataFrame.COLUMN_PARTITION_QID);
	
	private final JarveySession m_jarvey;
	private final FilePath m_path;
	private final MapTile m_tile;
	private final long m_length;
	private final Lazy<Long> m_count = Lazy.of(() -> read().count());
	
	public static final SpatialPartitionFile createOutlierFile(JarveySession jarvey, FilePath path) throws IOException {
		return new SpatialPartitionFile(jarvey, path, null);
	}
	
	SpatialPartitionFile(JarveySession jarvey, FilePath path, MapTile tile) throws IOException {
		m_jarvey = jarvey;
		m_path = path;
		m_tile = tile;
		m_length = calcLength(m_path);
	}
	
//	public static SpatialPartitionFile fromQuadId(SpatialClusterFile scFile, long quadId) throws IOException {
//		String fname = String.format("%s=%d", SpatialDataset.PRIMARY_CLUSTER_ID, quadId);
//		FilePath path = scFile.getFilePath().getChild(fname);
//		MapTile tile = (quadId == MapTile.QID_OUTLIER) ? null : MapTile.fromQuadId(quadId);
//		return new SpatialPartitionFile(path, tile);
//	}
//	
//	public static SpatialPartitionFile fromQuadKey(SpatialClusterFile scFile, String quadKey) throws IOException {
//		return fromQuadId(scFile, MapTile.toQuadId(quadKey));
//	}
	
	public static SpatialPartitionFile fromFilePath(JarveySession jarvey, FilePath partitionPath) throws IOException {
		long quadId = toQuadId(partitionPath);
		MapTile tile = (quadId == MapTile.OUTLIER_QID) ? null : MapTile.fromQuadId(quadId);
		return new SpatialPartitionFile(jarvey, partitionPath, tile);
	}
	
	public String getQuadKey() {
		return (m_tile != null) ? m_tile.getQuadKey() : MapTile.OUTLIER_QKEY;
	}
	
	public long getQuadId() {
		return (m_tile != null) ? m_tile.getQuadId() : MapTile.OUTLIER_QID;
	}
	
	public long getLength() {
		return m_length;
	}
	
	public boolean isOutlier() {
		return m_path.getName().equals(OUTLIER_FILE_NAME);
	}
	
	public long count() {
		return m_count.get();
	}
	
	public SpatialDataFrame read() {
		Dataset<Row> df = m_jarvey.read().parquet(m_path);
		
		long quadId = toQuadId(m_path);
		df = df.withColumn(SpatialDataFrame.COLUMN_PARTITION_QID, lit(quadId));
		
		FilePath schemaPath = m_path.getParent().getChild(JarveySession.SCHEMA_FILE);
		JarveySchema jschema = JarveyDataFrameReader.readJarveySchema(schemaPath);
		
		return m_jarvey.toSpatial(df, jschema);
		
//		String dsId = jarvey.toDatasetId(m_path.getParent());
//		String sqlExpr = String.format("%s = %d", SpatialDataset.PRIMARY_CLUSTER_ID, getQuadId());
//		return jarvey.read().dataset(dsId).where(sqlExpr);
	}
	
	public static long calcLength(FilePath path) throws IOException {
		return FStream.from(path.walkRegularFileTree())
						.mapOrThrow(FilePath::getLength)
						.mapToLong(DataUtils::asLong)
						.sum();
	}
	
	public static final boolean isPartitionFile(FilePath path) {
		return path.getName().startsWith(SpatialDataFrame.COLUMN_PARTITION_QID + "=");
	}
	
	public static final boolean isOutlierFile(FilePath path) {
		return path.getName().equals(OUTLIER_FILE_NAME);
	}
	
	private static long toQuadId(FilePath path) {
		String qidStr = path.getName().substring(SpatialDataFrame.COLUMN_PARTITION_QID.length()+1);
		return Long.parseLong(qidStr);
	}
	
	@Override
	public String toString() {
		String sdfId = m_jarvey.toDatasetId(m_path.getParent());
		return String.format("%s[%s(%d), %s]", sdfId, getQuadKey(), getQuadId(),
												UnitUtils.toByteSizeString(getLength()));
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() == SpatialPartitionFile.class ) {
			return false;
		}
		
		SpatialPartitionFile other = (SpatialPartitionFile)this;
		return Objects.equal(m_path, other.m_path);
	}

	@Override
	public int compareTo(SpatialPartitionFile o) {
		return (int)(o.m_length - m_length);
	}
}
