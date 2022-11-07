package jarvey.command;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.support.SchemaUtils;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.temporal.build.BuildTemporalPoint;
import jarvey.type.temporal.build.CreateTemporalPointIndex;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.PicocliSubCommand;
import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class TemporalPointCommands {
	@Command(name="build", description="build temporal-point dataset")
	public static class BuildTemporalPointCommand extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="input-id", index="0", arity="1..1", description={"input dataset id"})
		private String m_src;
		
		@Parameters(paramLabel="output-id", index="1", arity="1..1", description={"output dataset id"})
		private String m_dest;

		@Option(names= {"-k", "--key-cols"}, paramLabel="'key1,key2,...'", required=true,
				description="key columns (CSV format)")
		private void setKeyColumnNames(String keyColNames) {
			m_keyColNames = keyColNames.split(",");
		}
		private String[] m_keyColNames;

		@Option(names= {"-g", "--geom"}, paramLabel="'geom-col'", description="geometry column name")
		private void setGeomColName(String colName) {
			m_geomColName = colName;
		}
		private String m_geomColName = null;

		@Option(names= {"-t", "--ts"}, paramLabel="'ts'", defaultValue="ts",
				description="input columns for the point timestamp (default: 'ts')")
		private void setTsColName(String tsColName) {
			m_tsColName = tsColName;
		}
		private String m_tsColName;

		@Option(names= {"-s", "--segment_interval"}, paramLabel="interval", defaultValue="10m",
				description="segment interval (default: '10m')")
		private void setSegmentInterval(String interval) {
			m_segmentInterval = UnitUtils.parseDuration(interval);
		}
		private long m_segmentInterval;
		
		@Option(names={"-n", "--part-count"}, paramLabel="number", required=true,
				description="merging parition count")
		private int m_partCount;
		
		@Option(names={"-f", "--force"}, description="force to create a new output directory")
		private boolean m_force;
		
		@Option(names={"-v", "-verbose"}, description="verbose")
		private boolean m_verbose = false;
		
		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();

			Dataset<Row> input;
			SpatialDataFrame sdf = jarvey.read().dataset(m_src);

			JarveyDataType tsType = sdf.getJarveySchema()
										.findColumn(m_tsColName)
										.map(JarveyColumn::getJarveyDataType)
										.getOrNull();
			if ( tsType == null ) {
				throw new IllegalArgumentException("invalid timestamp column: " + m_tsColName);
			}
			else if ( tsType != JarveyDataTypes.Timestamp_Type && tsType != JarveyDataTypes.Long_Type ) {
				throw new IllegalArgumentException("invalid timestamp column type: " + tsType);
			}
			
			String[] xytColNames = new String[]{null, null, m_tsColName};
			if ( m_geomColName == null ) {
				sdf.assertDefaultGeometryColumnInfo();
				sdf = sdf.to_xy("_x", "_y");
				xytColNames = new String[]{"_x", "_y", m_tsColName};
			}
			else {
				String[] parts = m_geomColName.split(",");
				if ( parts.length == 1 ) {
					sdf = sdf.setDefaultGeometryColumn(parts[0]).to_xy("_x", "_y");
					xytColNames = new String[]{"_x", "_y", m_tsColName};
				}
				else {
					xytColNames = new String[]{parts[0], parts[1], m_tsColName};
				}
			}
			input = sdf.toDataFrame();
			
			BuildTemporalPoint app = new BuildTemporalPoint(jarvey, m_keyColNames, xytColNames, m_dest);
			app.setPeriod(m_segmentInterval);
			app.setMergingPartitionCount(m_partCount);
			app.run(input);
			watch.stop();
			
			if ( m_verbose ) {
				Dataset<Row> tpoints = jarvey.read().dataset(m_dest).toDataFrame();
				
				long ntpoints = tpoints.count();
				Column[] keyCols = FStream.of(m_keyColNames).map(tpoints::col).toArray(Column.class);
				long nkeys = tpoints.select(keyCols).distinct().count();
				
				System.out.printf("built: dataset=%s, output=%s, count=%d, nkeys=%d, elapsed=%s%n",
									m_src, m_dest, ntpoints, nkeys, watch.getElapsedMillisString());
				System.in.read();
			}
		}
	}
	
	@Command(name="index", description="create temporal-point dataset index")
	static class CreateTemporalPointIndexCommand extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="input-id", index="0", arity="1..1", description={"input tpoint-dataset id"})
		private String m_src;
		
		@Parameters(paramLabel="output-id", index="1", arity="1..1", description={"output index id"})
		private String m_dest;

		@Option(names= {"-q", "--quad-id"}, paramLabel="'ds_id", required=true,
				description="quad-id set id")
		private String m_qidSetId;
		
		@Option(names={"-n", "--part-count"}, paramLabel="number", defaultValue="1",
				description="output parition count")
		private int m_partCount;
		
		@Option(names={"-f", "--force"}, description="force to create a new output directory")
		private boolean m_force;
		
		@Option(names={"-v", "-verbose"}, description="verbose")
		private boolean m_verbose = false;
		
		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();

			Dataset<Row> input = jarvey.read().dataset(m_src).toDataFrame();
			long[] qids = jarvey.read().quadSpaceIds(m_qidSetId);
			CreateTemporalPointIndex app = new CreateTemporalPointIndex(jarvey, qids, m_dest, m_partCount);
			app.run(input);
			watch.stop();
			
			if ( m_verbose ) {
				Dataset<Row> idx = jarvey.read().dataset(m_dest).toDataFrame();
				long ntpoints = idx.count();
				
				String[] keyColNames = SchemaUtils.complement(input.schema(), "tpoint").fieldNames();
				Column[] keyCols = FStream.of(keyColNames).map(idx::col).toArray(Column.class);
				long nkeys = idx.select(keyCols).distinct().count();
				
				System.out.printf("built: dataset=%s, count=%d, nkeys=%d, elapsed=%s%n",
									m_dest, ntpoints, nkeys, watch.getElapsedMillisString());
				System.in.read();
			}
		}
	}
}
