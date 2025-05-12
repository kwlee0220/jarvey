package jarvey.command;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import utils.CSV;
import utils.PicocliSubCommand;
import utils.StopWatch;
import utils.Tuple;
import utils.UnitUtils;
import utils.io.FilePath;
import utils.stream.FStream;

import jarvey.DatasetType;
import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.cluster.ClusterDataset;
import jarvey.cluster.ClusterDatasetOptions;
import jarvey.cluster.EstimateQuadSpaces;
import jarvey.cluster.EstimateQuadSpaces.PartitionEstimate;
import jarvey.cluster.EstimateQuadSpacesOptions;
import jarvey.cluster.SpatialClusterFile;
import jarvey.cluster.SpatialPartitionFile;
import jarvey.command.TemporalPointCommands.BuildTemporalPointCommand;
import jarvey.command.TemporalPointCommands.CreateTemporalPointIndexCommand;
import jarvey.command.TestCommands.BuildSafeZoneSpeedCommand;
import jarvey.command.TestCommands.BuildTenMinutePolicyCommand;
import jarvey.command.TestCommands.CountInvalidGeomCommand;
import jarvey.command.TestCommands.FindBestSubwayStationsCommand;
import jarvey.datasource.CsvParameters;
import jarvey.datasource.JarveyDataFrameReader;
import jarvey.datasource.JarveyDataFrameWriter;
import jarvey.datasource.shp.ExportShapefileParameters;
import jarvey.datasource.shp.ShapefileDataSets;
import jarvey.datasource.shp.ShapefileParameters;
import jarvey.datasource.shp.ShapefileWriter;
import jarvey.optor.geom.join.SpatialJoinOptions;
import jarvey.support.MapTile;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveySchema;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DatasetCommands {
	@Command(name="list", description="list datasets")
	public static class ListDataSet extends PicocliSubCommand<JarveySession> {
		@Option(names={"-l"}, description="list in detail")
		private boolean m_details;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			FStream.from(jarvey.listDatasets())
					.forEach(dsId -> {
						System.out.print(dsId);
						
						if ( m_details ) {
							SpatialDataFrame sds = jarvey.read().dataset(dsId);
							GeometryColumnInfo gcInfo = sds.getDefaultGeometryColumnInfo();
							if ( gcInfo != null ) {
								System.out.printf(": %s", gcInfo);
							}
							
							long nrows = sds.count();
							System.out.printf(", count=%s", nrows);

							long nbytes = jarvey.calcDatasetLength(dsId);
							System.out.printf(", size=%s", UnitUtils.toByteSizeString(nbytes));
						}
						System.out.println();
					});
		}
	}

	@Command(name="show", description="print records of the dataset")
	public static class Show extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id to print"})
		private String m_dsId;

		@Option(names={"-t", "--type"}, paramLabel="type",
				description="target type: dataset (default), file, thumbnail")
		private String m_type = "dataset";

		@Option(names={"--project"}, paramLabel="column_list", description="selected columns (optional)")
		private String m_cols = null;
		
		@Option(names={"--limit"}, paramLabel="count", description="limit count (optional)")
		private int m_limit = -1;

		@Option(names={"--csv"}, description="display csv format")
		private boolean m_asCsv;

		@Option(names={"--delim"}, paramLabel="character", description="csv delimiter (default: ',')")
		private String m_delim = ",";

		@Option(names={"-g", "--geom"}, description="display geometry columns")
		private boolean m_displayGeom;

		@Option(names={"--header"}, description="display column names")
		private boolean m_header;
		
		@Override
		public void run(JarveySession jarvey) throws Exception {
			SpatialDataFrame sds = jarvey.read().dataset(m_dsId);
			
			if ( m_limit > 0 ) {
				sds = sds.limit(m_limit);
			}
			if ( m_cols != null ) {
				String[] cols = CSV.parseCsvAsArray(m_cols);
				sds = sds.select(cols);
			}

			List<JarveyColumn> columns = sds.getJarveySchema().getColumnAll();
			boolean[] mask = new boolean[columns.size()];
			Arrays.fill(mask, true);
			if ( !m_displayGeom ) {
				FStream.from(columns)
						.zipWithIndex()
						.filter(t -> t.value().getJarveyDataType().isGeometryType())
						.forEach(t -> mask[t.index()] = false);
			}
			
			
			List<JarveyDataType> jtypes = FStream.from(sds.getJarveySchema().getColumnAll())
												.map(JarveyColumn::getJarveyDataType)
												.toList();
			FStream.from(sds.toLocalIterator())
					.forEach(row -> {
						List<Object> values = Lists.newArrayList();
						for ( int i =0; i < row.length(); ++i ) {
							if ( mask[i] ) {
								values.add(jtypes.get(i).deserialize(row.get(i)));
							}
						}
						
						if ( m_asCsv ) {
							System.out.println(toCsv(values, m_delim));
						}
						else {
							System.out.println(values);
						}
					});
		}
		
		private static String toCsv(Collection<?> values, String delim) {
			return values.stream()
						.map(o -> {
							String str = ""+o;
							if ( str.contains(" ") || str.contains(delim) ) {
								str = "\"" + str + "\"";
							}
							return str;
						})
						.collect(Collectors.joining(delim));
		}
	}

	@Command(name="schema", description="print the RecordSchema of the dataset")
	public static class Schema extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Option(names={"-c", "--cluster"}, description="display cluster ids")
		private boolean m_showClusters = false;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			SpatialDataFrame sds = jarvey.read().dataset(m_dsId);
			
			JarveySchema jschema = jarvey.loadJarveySchema(m_dsId);
			if ( jschema != null ) {
				System.out.println("COUNT         : " + sds.count());
				
				long totalSize = jarvey.calcDatasetLength(m_dsId);
				System.out.println("SIZE          : " + UnitUtils.toByteSizeString(totalSize));
				
				if ( jschema.getQuadIds() != null ) {
					if ( m_showClusters ) {
						String clusters = FStream.of(jschema.getQuadIds())
												.map(MapTile::toQuadKey)
												.join(", ");
						System.out.printf("CLUSTERS (%3d): %s\n", jschema.getQuadIds().length, clusters);
					}
					else {
						System.out.printf("CLUSTERS      : %d\n", jschema.getQuadIds().length);
					}
				}
			}
			else {
				System.out.println("TYPE          : " + DatasetType.CSV);
				System.out.println("COUNT         : unknown");
				
				long totalSize = jarvey.calcDatasetLength(m_dsId);
				System.out.println("SIZE          : " + UnitUtils.toByteSizeString(totalSize));
			}
			
			GeometryColumnInfo gcInfo = sds.getDefaultGeometryColumnInfo();
			if ( gcInfo != null ) {
				System.out.println("GEOMETRY      : " + gcInfo);
			}
			
			System.out.println("COLUMNS       :");
			FStream.from(sds.getJarveySchema().getColumnAll())
					.forEach(col -> System.out.printf("  %-12s: %s\n", col.getName(), col.getJarveyDataType()));
		}
	}
	
	@Command(name="quadspace", description="create quad-space partitions for the dataset")
	public static class SplitQuadspace extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Option(names= {"--sample_ratio"}, paramLabel="ratio", description="sampling ratio (default: 0.3)")
		private void setSampleRatio(double ratio) {
			m_sampleRatio = ratio;
		}
		private double m_sampleRatio = 0.3;

		@Option(names= {"--cluster_size"}, paramLabel="size", description="cluster size (default: '64mb')")
		private void setClusterSize(String sizeStr) {
			m_clusterSize = UnitUtils.parseByteSize(sizeStr);
		}
		private long m_clusterSize = UnitUtils.parseByteSize("64mb");

		@Option(names= {"--cluster_limit"}, paramLabel="size", description="cluster size upper-limit (default: '80mb')")
		private void setClusterUpperLimit(String sizeStr) {
			m_clusterLimit = UnitUtils.parseByteSize(sizeStr);
		}
		private long m_clusterLimit = UnitUtils.parseByteSize("80mb");

		@Option(names= {"--outlier_limit"}, paramLabel="nbytes", description="outlier size limit (default: '2mb')")
		private void setOutlierLimit(String sizeStr) {
			m_outlierLimit = UnitUtils.parseByteSize(sizeStr);
		}
		private long m_outlierLimit = UnitUtils.parseByteSize("2mb");
	
		@Option(names={"--save"}, description="save estimated quad-space's ids")
		private boolean m_save;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			EstimateQuadSpacesOptions opts = EstimateQuadSpacesOptions.create();
			opts = opts.sampleRatio(m_sampleRatio)
						.clusterSizeHint(m_clusterSize)
						.clusterSizeLimit(m_clusterLimit)
						.outlierSizeLimit(m_outlierLimit)
						.save(m_save);
			
			EstimateQuadSpaces estimate = new EstimateQuadSpaces(jarvey, m_dsId, opts);
			Tuple<Integer, TreeSet<PartitionEstimate>> result = estimate.call();
			watch.stop();
			
			for ( PartitionEstimate qspace: result._2 ) {
				System.out.printf("%s%n", qspace);
			}
		}
	}
	
	@Command(name="cluster", description="cluster dataset")
	public static class Cluster extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Option(names= {"--ref_dataset"}, paramLabel="dataset_id", description="reference dataset id")
		private void setReferenceDataset(String dsId) {
			m_refDsId = dsId;
		}
		private String m_refDsId = null;

		@Option(names= {"--sample_ratio"}, paramLabel="ratio", description="sampling ratio (default: 0.3)")
		private void setSampleRatio(double ratio) {
			m_sampleRatio = ratio;
		}
		private double m_sampleRatio = 0.3;

		@Option(names= {"--cluster_size"}, paramLabel="size", defaultValue="64mb",
				description="cluster size (default: '64mb')")
		private void setClusterSize(String sizeStr) {
			m_clusterSize = UnitUtils.parseByteSize(sizeStr);
		}
		private long m_clusterSize;

		@Option(names= {"--cluster_limit"}, paramLabel="size", defaultValue="none",
				description="cluster size upper-limit (default: none)")
		private void setClusterUpperLimit(String sizeStr) {
			m_clusterLimit = sizeStr.equals("none") ? -1 : UnitUtils.parseByteSize(sizeStr);
		}
		private long m_clusterLimit;

		@Option(names= {"--outlier_limit"}, paramLabel="nbytes", defaultValue="2mb",
				description="outlier size limit (default: '2mb')")
		private void setOutlierLimit(String sizeStr) {
			m_outlierLimit = UnitUtils.parseByteSize(sizeStr);
		}
		private long m_outlierLimit;
	
		@Option(names={"--drop_final_outliers"}, description="drop outlier finally")
		private boolean m_dropFinalOutlier = false;
	
		@Option(names={"-f", "--force"}, description="force to create clustered dataset")
		private boolean m_force;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			ClusterDatasetOptions opts = ClusterDatasetOptions.create().force(m_force);
			if ( m_refDsId != null ) {
				JarveySchema jschema = jarvey.loadJarveySchema(m_refDsId);
				opts = opts.candidateQuadIds(jschema.getQuadIds());
			}
			opts = opts.clusterSizeHint(m_clusterSize);
			opts = opts.clusterSizeLimit(m_clusterLimit);
			opts = opts.outlierSizeLimit(m_outlierLimit);
			opts = opts.dropFinalOutliers(m_dropFinalOutlier);
			opts = opts.sampleRatio(m_sampleRatio);
			
			ClusterDataset cluster = new ClusterDataset(jarvey, m_dsId, opts);
			SpatialClusterFile scFile = cluster.call();
			scFile.streamPartitions(false).forEach(System.out::println);
			
			System.out.printf("clustered: dataset=%s count=%d elapsed=%s%n",
								m_dsId, scFile.getPartitionCount(), watch.getElapsedMillisString());
		}
	}

	@Command(name="move", description="move a dataset to another directory")
	public static class Move extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"id for the source dataset"})
		private String m_src;
		
		@Parameters(paramLabel="new_id", index="1", arity="1..1", description={"new id"})
		private String m_dest;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			FilePath srcDsPath = jarvey.getDatasetFilePath(m_src);
			FilePath destDsPath = jarvey.getDatasetFilePath(m_dest);
			srcDsPath.renameTo(destDsPath, false);
		}
	}

	@Command(name="copy", description="copy a dataset")
	public static class Copy extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"id for the source dataset"})
		private String m_src;
		
		@Parameters(paramLabel="new_id", index="1", arity="1..1", description={"new id"})
		private String m_dest;

		@Option(names= {"-n", "--part_count"}, paramLabel="count", defaultValue="-1",
				description="partition count for the copied dataset (default: -1)")
		private void setPartitionCount(int count) {
			m_partCount = count;
		}
		private int m_partCount;

		@Option(names={"-f"}, description="force to create a dataset")
		private boolean m_force = false;
		
		@Option(names={"-v", "-verbose"}, description="verbose")
		private boolean m_verbose = false;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			SpatialDataFrame src = jarvey.read().dataset(m_src);
			if ( m_partCount > 0 && m_partCount < src.getPartitionCount() ) {
				src.coalesce(m_partCount)
					.writeSpatial().force(m_force).dataset(m_dest);
			}
			else if ( m_partCount <= 0 || m_partCount == src.getPartitionCount() ) {
				src.writeSpatial().force(m_force).dataset(m_dest);
			}
			else {
				String msg = String.format("Target partition count (%d) is larger than that of source count (%d)",
											m_partCount, src.getPartitionCount());
				throw new IllegalArgumentException(msg);
			}
			
			if ( m_verbose ) {
				System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
			}
		}
	}

	@Command(name="set_sdsinfo", description="set SpatialDataset information")
	public static class SetSpatialDatasetInfo extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			SpatialDataFrame sds = jarvey.read().dataset(m_dsId);
			GeometryColumnInfo gcInfo = sds.getDefaultGeometryColumnInfo();
			StructType schema = sds.schema();
			
//			SpatialDatasetInfo sdInfo = new SpatialDatasetInfo(DatasetType.CSV, gcInfo, schema);
//			jarvey.saveSpatialDatasetInfo(m_dsId, sdInfo);
		}
	}

	@Command(name="count", description="count records of the dataset")
	public static class Count extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;
		
		@Option(names={"-v", "-verbose"}, description="verbose")
		private boolean m_verbose = false;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			long cnt = jarvey.read().dataset(m_dsId).count();
			
			if ( m_verbose ) {
				System.out.printf("count=%d, elapsed=%s%n", cnt, watch.getElapsedMillisString());
			}
			else {
				System.out.println(cnt);
			}
		}
	}

	@Command(name="delete", description="delete the dataset(s)")
	public static class Delete extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			FilePath dsPath = jarvey.getDatasetFilePath(m_dsId);
			dsPath.delete();
		}
	}

	@Command(name="cluster_info", description="display cluster infomations")
	public static class ClusterInfo extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			SpatialClusterFile scFile = SpatialClusterFile.fromDataset(jarvey, m_dsId);
			int nparts = scFile.getPartitionCount();
			scFile.streamPartitions(false)
					.zipWithIndex()
					.forEach(t -> {
						int index = t.index() + 1;
						SpatialPartitionFile spFile = t.value();
						System.out.printf("[%2d/%2d]: %17s(%8d): count=%d, size=%s\n",
										index, nparts,
										spFile.getQuadKey(), spFile.getQuadId(), spFile.count(),
										UnitUtils.toByteSizeString(spFile.getLength()));
					});
			System.out.printf("total partition count: %d%n", scFile.getPartitionCount());
		}
	}

	@Command(name="import",
			subcommands= {
//				ImportCsvCmd.class,
				ImportShapefileCmd.class,
//				ImportGeoJsonCmd.class,
//				ImportJdbcCmd.class
			},
			description="import into the dataset")
	public static class Import extends PicocliSubCommand<JarveySession> {
		@Override
		public void run(JarveySession marmot) throws Exception { }
	}

//	@Command(name="csv", description="import CSV file into the dataset")
//	public static class ImportCsvCmd extends PicocliSubCommand<MarmotRuntime> {
//		@Mixin private CsvParameters m_csvParams;
//		
//		@Parameters(paramLabel="file_path", index="0", arity="1..1",
//					description={"path to the target csv file"})
//		private File m_start;
//		
//		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
//					description={"dataset id to import onto"})
//		private String m_dsId;
//		
//		@Option(names={"--glob"}, paramLabel="expr", description="glob expression for import files")
//		private String m_glob = "**/*.csv";
//
//		@Option(names={"-f"}, description="force to create a dataset")
//		private boolean m_force = false;
//
//		@Override
//		public void run(MarmotRuntime marmot) throws Exception {
//			StopWatch watch = StopWatch.start();
//
//			RecordReader reader = CsvRecordReader.from(m_start, m_csvParams, m_glob);
//			reader = m_csvParams.pointColumns().transform(reader, (r, tup) -> {
//				r = r.addGeometryPoint(tup._3, tup._4, tup._1, tup._2);
//				r = r.project(String.format("%s,*-{%s,%s,%s}", tup._1, tup._1, tup._3, tup._4));
//				return r;
//			});
//			
//			DataSetInfo info = new DataSetInfo(m_dsId, DataSetType.AVRO, reader.getRecordSchema());
//			DataSet ds = marmot.getDataSetServer().createDataSet(info, m_force);
//			long count = ds.write(reader.read());
//			
//			double velo = count / watch.getElapsedInFloatingSeconds();
//			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
//								m_dsId, count, watch.getElapsedMillisString(), velo);
//		}
//	}

	@Command(name="shp", aliases={"shapefile"}, description="import shapefile(s) into the dataset")
	public static class ImportShapefileCmd extends PicocliSubCommand<JarveySession> {
		@Mixin private ShapefileParameters m_shpParams;
		
		@Parameters(paramLabel="file_path", index="0", arity="1..1",
					description={"path to the target csv file"})
		private File m_start;
		
		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
					description={"dataset id to import onto"})
		private String m_dsId;

		@Option(names={"-f"}, description="force to create a dataset")
		private boolean m_force = false;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();

			JarveyDataFrameReader reader = jarvey.read();
			reader = reader.option("charset", m_shpParams.charset().toString());
			reader = m_shpParams.srid().transform(reader, (r, shpSrid) -> r.option("srid", ""+shpSrid));
			reader = m_shpParams.paritionCount().transform(reader, (r, n) -> r.option("nparts", ""+n));

			int srid = m_shpParams.srid().getOrElseThrow(() -> {
				SimpleFeatureType sfType = ShapefileDataSets.getSimpleFeatureType(m_start);
				return ShapefileDataSets.getSrid(sfType);
			});
			
			SpatialDataFrame sds = reader.shapefile(m_start);
			JarveyDataFrameWriter writer = sds.writeSpatial();
			if ( m_force ) {
				writer = writer.mode(SaveMode.Overwrite);
			}
			writer.dataset(m_dsId);
			watch.stop();
			
			long count = jarvey.read().dataset(m_dsId).count();
			double velo = count / watch.getElapsedInFloatingSeconds();
			
			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
								m_dsId, count, watch.getElapsedMillisString(), velo);
		}
	}
	
	@Command(name="export",
			subcommands= {
				ExportCsv.class,
				ExportShapefile.class,
//				ExportGeoJson.class,
//				ExportJdbcTable.class,
			},
			description="export a dataset")
	public static class Export extends PicocliSubCommand<JarveySession> {
		@Override
		public void run(JarveySession initialContext) throws Exception { }
	}
	
	@Command(name="csv", description="export a dataset in CSV format")
	public static class ExportCsv extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to export"})
		private String m_dsId;

		@Parameters(paramLabel="file_path", index="1", arity="0..1",
					description={"file path for exported CSV file"})
		private String m_output;

		@Mixin private CsvParameters m_params;
		
		@Option(names={"-f"}, description="delete the file if it exists already")
		private boolean m_force;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			SpatialDataFrame sdf = jarvey.read().dataset(m_dsId);

			GeometryColumnInfo gcInfo = sdf.getDefaultGeometryColumnInfo();
			String pointCols = m_params.pointCols();
			if ( pointCols != null && gcInfo != null ) {
				String[] xyCols = pointCols.split(",");
				sdf = sdf.to_xy(xyCols[0], xyCols[1])
						.drop(gcInfo.getName());
			}
			else {
				sdf = sdf.to_wkt(gcInfo.getName());
			}
			
			String fullPath = jarvey.getDatasetRootPath().path(m_output).getAbsolutePath();
			sdf.toDataFrame()
				.write()
				.options(m_params.options())
				.mode(m_force ? SaveMode.Overwrite : SaveMode.ErrorIfExists)
				.csv(fullPath);
		}
	}

	@Command(name="shp", description="export the dataset in Shapefile format")
	public static class ExportShapefile extends PicocliSubCommand<JarveySession> {
		@Mixin private ExportShapefileParameters m_shpParams;
		
		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
					description={"dataset id to export"})
		private String m_dsId;

		@Parameters(paramLabel="output_dir", index="1", arity="1..1",
					description={"directory path for the output shapefiles"})
		private File m_output;
		
		@Option(names={"-f", "--force"}, description="force to create a new output directory")
		private boolean m_force;
		
		@Override
		public void run(JarveySession jarvey) throws Exception {
			SpatialDataFrame sds = jarvey.read().dataset(m_dsId);
			
			int srid = sds.getDefaultGeometryColumnInfo().getSrid();
			Charset charset = m_shpParams.charset();

			int shpSrid = m_shpParams.shpSrid().getOrElse(0);
			if ( shpSrid != 0 && shpSrid != srid ) {
				sds = sds.transformCrs(shpSrid);
			}
			ShapefileWriter writer = ShapefileWriter.get(m_output, srid, charset)
													.setForce(m_force);
			writer.write(sds);
		}
	}

////	@Command(name="geojson", description="export a dataset in GeoJSON format")
////	public static class ExportGeoJson extends PicocliSubCommand<MarmotRuntime> {
////		@Mixin private GeoJsonParameters m_gjsonParams;
////		
////		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
////					description={"dataset id to export"})
////		private String m_dsId;
////
////		@Parameters(paramLabel="file_path", index="1", arity="0..1",
////					description={"file path for exported GeoJson file"})
////		private String m_output;
////		
////		@Option(names={"-p", "-pretty"}, description={"path to the output CSV file"})
////		private boolean m_pretty;
////
////		@Override
////		public void run(MarmotRuntime initialContext) throws Exception {
////			ExportAsGeoJson export = new ExportAsGeoJson(m_dsId)
////										.printPrinter(m_pretty);
////			m_gjsonParams.geoJsonSrid().ifPresent(export::setGeoJSONSrid);
////			
////			FOption<String> output = FOption.ofNullable(m_output);
////			BufferedWriter writer = ExternIoUtils.toWriter(output, m_gjsonParams.charset());
////			long count = export.run(initialContext, writer);
////			
////			System.out.printf("done: %d records%n", count);
////		}
////	}
////
////	@Command(name="jdbc", description="export a dataset into JDBC table")
////	public static class ExportJdbcTable extends PicocliSubCommand<MarmotRuntime> {
////		@Mixin private StoreJdbcParameters m_jdbcParams;
////		
////		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
////					description={"dataset id to export"})
////		private String m_dsId;
////
////		@Parameters(paramLabel="table_name", index="1", arity="1..1",
////					description={"JDBC table name"})
////		private String m_tblName;
////		
////		@Option(names={"-report_interval"}, paramLabel="record count",
////				description="progress report interval")
////		private int m_interval = -1;
////
////		@Override
////		public void run(MarmotRuntime initialContext) throws Exception {
////			ExportIntoJdbcTable export = new ExportIntoJdbcTable(m_dsId, m_tblName, m_jdbcParams);
////			FOption.when(m_interval > 0, m_interval)
////					.ifPresent(export::reportInterval);
////			
////			long count = export.run(initialContext);
////			System.out.printf("done: %d records%n", count);
////		}
////	}

	@Command(name="tag", description="tag the dataset with region id")
	public static class TagWithRegion extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="input_dataset", index="0", arity="1..1", description={"input dataset id"})
		private String m_dsId;

		@Parameters(paramLabel="region_dataset", index="1", arity="1..1", description={"region dataset id"})
		private String m_regionDsId;
		
		@Parameters(paramLabel="output_dataset", index="2", arity="1..1", description={"output dataset id"})
		private String m_outputDsId;

		private Set<String> m_regionKeyCols;
		@Option(names={"--region_keys"}, paramLabel="cols", required=true, description="region key columns (in CSV)")
		public void setRegionKeyCols(String colsStr) {
			m_regionKeyCols = Sets.newHashSet(colsStr.split(","));
		}
		
		@Option(names={"-f", "--force"}, description="force to create a new output directory")
		private boolean m_force;
		
		@Option(names={"-v", "-verbose"}, description="verbose")
		private boolean m_verbose = false;
		
		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			SpatialDataFrame sdf = jarvey.read().dataset(m_dsId);
			int srid = sdf.assertDefaultGeometryColumnInfo().getSrid();
			
			SpatialDataFrame regionSdf = jarvey.read().dataset(m_regionDsId);
			GeometryColumnInfo regionGeomInfo = regionSdf.assertDefaultGeometryColumnInfo();
			
			Set<String> refColSet = Sets.newHashSet(m_regionKeyCols);
			refColSet.add(regionGeomInfo.getName());
			String[] refCols = FStream.from(refColSet).toArray(String.class);
			regionSdf = regionSdf.select(refCols);
			if ( srid != regionGeomInfo.getSrid() ) {
				regionSdf = regionSdf.transformCrs(srid);
			}

			SpatialJoinOptions opts = SpatialJoinOptions.OUTPUT("left.*,right.*-{the_geom}");
			SpatialDataFrame output = sdf.spatialBroadcastJoin(regionSdf, opts);
			
			output.writeSpatial().force(m_force).dataset(m_outputDsId);
			if ( m_verbose ) {
				System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
			}
		}
	}

	@Command(name="tag_geometry", description="tag geometry the dataset from reference geometry dataset")
	public static class TagGeometry extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="input_dataset", index="0", arity="1..1", description={"input dataset id"})
		private String m_dsId;

		@Parameters(paramLabel="geometry_dataset", index="1", arity="1..1", description={"geometry dataset id"})
		private String m_geomDsId;
		
		@Parameters(paramLabel="output_dataset", index="2", arity="1..1", description={"output dataset id"})
		private String m_outputDsId;

		private Tuple<String,String> m_joinColPair;
		@Option(names={"--join_columns"}, paramLabel="join column pair", required=true,
				description="join column pairs (separater=':')")
		public void setRegionKeyCols(String colsExpr) {
			String[] pair = colsExpr.split(":");
			if ( pair.length != 2 ) {
				throw new IllegalArgumentException(String.format("invalid join column pair: '%s'", colsExpr));
			}
			
			m_joinColPair = Tuple.of(pair[0], pair[1]);
		}

		@Option(names={"--nparts","-n"}, paramLabel="count", description="output parition count")
		private int m_partCount = -1;
		
		@Option(names={"-f", "--force"}, description="force to create a new output directory")
		private boolean m_force;
		
		@Option(names={"-v", "-verbose"}, description="verbose")
		private boolean m_verbose = false;
		
		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			SpatialDataFrame df = jarvey.read().dataset(m_dsId);
			SpatialDataFrame geomDf = jarvey.read().dataset(m_geomDsId);
			Column joinExpr = df.col(m_joinColPair._1).equalTo(geomDf.col(m_joinColPair._2));
			
			GeometryColumnInfo gcInfo = geomDf.assertDefaultGeometryColumnInfo();
			Column[] outputCols = FStream.of(geomDf.col(gcInfo.getName()))
										.concatWith(FStream.of(df.schema().fieldNames()).map(df::col))
										.toArray(Column.class);
			Dataset<Row> outDf = df.toDataFrame().join(geomDf.toDataFrame(), joinExpr, "leftouter")
									.select(outputCols);
			
			JarveyColumn geomCol = geomDf.getJarveySchema().getColumn(gcInfo.getName());
			JarveySchema outSchema = df.getJarveySchema().toBuilder()
										.addJarveyColumn(0, geomCol.getName().get(), geomCol.getJarveyDataType())
										.build();
			SpatialDataFrame output = jarvey.toSpatial(outDf, outSchema);
			if ( m_partCount > 0 ) {
				output = output.coalesce(m_partCount);
			}
			
			output.writeSpatial().force(m_force).dataset(m_outputDsId);
			if ( m_verbose ) {
				System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
			}
		}
	}
	
	@Command(name="test",
			subcommands= {
				CountInvalidGeomCommand.class,
				BuildSafeZoneSpeedCommand.class,
				FindBestSubwayStationsCommand.class,
				BuildTenMinutePolicyCommand.class,
			},
			description="test commands")
	public static class TestCommands extends PicocliSubCommand<JarveySession> {
		@Override
		public void run(JarveySession initialContext) throws Exception { }
	}
	
	@Command(name="tpoint",
			subcommands= {
				BuildTemporalPointCommand.class,
				CreateTemporalPointIndexCommand.class,
			},
			description="TemporalPoint related commands")
	public static class TemporalPointCommands extends PicocliSubCommand<JarveySession> {
		@Override
		public void run(JarveySession initialContext) throws Exception { }
	}
}
