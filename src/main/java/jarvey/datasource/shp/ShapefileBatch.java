package jarvey.datasource.shp;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;

import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.Tuple;
import utils.UnitUtils;
import utils.func.FOption;
import utils.geo.Shapefile;
import utils.stream.FStream;


/**
 *
 * @author Kang-Woo Lee
 */
class ShapefileBatch implements Batch {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileBatch.class);
	
	private final JarveySchema m_jschema;
	private final int[] m_mapping;
	private final File m_start;
	private final String m_charset;
	private final InputPartition[] m_partitions;
	
	ShapefileBatch(StructType schema, int[] mapping, CaseInsensitiveStringMap options)
		throws IOException {
		GeometryType geomType = (mapping == null || mapping[0] == 0)
								? ShapefileDataSets.loadDefaultGeometryType(options) : null;
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", geomType);
		m_jschema = JarveySchema.fromStructType(schema, gcInfo);
		
		m_mapping = mapping;
		m_start = new File(options.get("path"));
		m_charset = FOption.ofNullable(options.get("charset")).getOrElse("UTF-8");
		
		List<File> shpFiles = Shapefile.traverseShpFiles(m_start).toList();
		int nparts = options.getInt("nparts", shpFiles.size());
		nparts = (nparts > 0) ? Math.min(nparts, shpFiles.size()) : shpFiles.size();
		
		PriorityQueue<MultiFiles> queue = new PriorityQueue<>(nparts);
		FStream.range(0, nparts)
				.map(MultiFiles::new)
				.forEach(queue::add);
		FStream.from(shpFiles)
				.mapOrThrow(shp -> Tuple.of(shp, calcShapefileLength(shp)))
				.sort(s_comparator)
				.forEach(tup -> {
					MultiFiles part = queue.poll();
					part.add(tup);
					queue.add(part);
				});
		m_partitions = FStream.from(queue.iterator())
								.map(p -> new MultiShapefilesPartition(p.m_index, p.m_files))
								.toArray(InputPartition.class);
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("create batch-scan: dir={}, nparts={}, nfiles={}", m_start, nparts, shpFiles.size());
		}
	}

	@Override
	public InputPartition[] planInputPartitions() {
		return m_partitions;
	}

	@Override
	public PartitionReaderFactory createReaderFactory() {
		return new MultiShapefilesReaderFactory(m_jschema, m_charset, m_mapping);
	}
	
	@Override
	public String toString() {
		return String.format("ShapefileBatch: start_path=%s, charset=%s, nparts=%d",
								m_start, m_charset, m_partitions.length);
	}
	
	private long calcShapefileLength(File shpFile) throws IOException {
		Shapefile shp = Shapefile.of(shpFile);
		return shp.getDbfFile().length() + shp.getShpFile().length();
	}
	
	private static class MultiFiles implements Comparable<MultiFiles> {
		private int m_index;
		private List<File> m_files;
		private long m_total;
		
		MultiFiles(int index) {
			m_index = index;
			m_files = Lists.newArrayList();
			m_total = 0;
		}
		
		void add(Tuple<File, Long> tup) {
			m_files.add(tup._1);
			m_total += tup._2;
		}

		@Override
		public int compareTo(MultiFiles o) {
			return (int)(m_total- o.m_total);
		}
		
		@Override
		public String toString() {
			return String.format("[%d]: count=%d, size=%s", m_index, m_files.size(),
									UnitUtils.toByteSizeString(m_total));
		}
	}
	
	private static final Comparator<Tuple<File,Long>> s_comparator = new Comparator<Tuple<File,Long>>() {
		@Override
		public int compare(Tuple<File,Long> o1, Tuple<File,Long> o2) {
			return (int)(o1._2 - o2._2);
		}
	};
}
