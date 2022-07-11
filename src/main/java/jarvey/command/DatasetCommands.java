package jarvey.command;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

import avro.shaded.com.google.common.collect.Lists;
import jarvey.DatasetType;
import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.jarvey_functions;
import jarvey.cluster.ClusterDataset;
import jarvey.cluster.ClusterDatasetOptions;
import jarvey.datasource.JarveyDataFrameReader;
import jarvey.datasource.JarveyDataFrameWriter;
import jarvey.datasource.shp.ExportShapefileParameters;
import jarvey.datasource.shp.ShapefileDataSets;
import jarvey.datasource.shp.ShapefileParameters;
import jarvey.datasource.shp.ShapefileWriter;
import jarvey.support.HdfsPath;
import jarvey.support.MapTile;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveySchema;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.CSV;
import utils.PicocliSubCommand;
import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

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
							SpatialDataset sds = jarvey.read().dataset(dsId);
							GeometryColumnInfo gcInfo = sds.getDefaultGeometryColumnInfo();
							if ( gcInfo != null ) {
								System.out.printf(": %s", gcInfo);
							}
							
							long nrows = sds.count();
							System.out.printf(", count=%s", nrows);

							long nbytes = jarvey.getDatasetSize(dsId);
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
		
		@Override
		public void run(JarveySession jarvey) throws Exception {
			SpatialDataset sds = jarvey.read().dataset(m_dsId);
			
			if ( m_limit > 0 ) {
				sds = sds.limit(m_limit);
			}
			if ( m_cols != null ) {
				Column[] cols = CSV.parseCsv(m_cols).map(sds::col).toArray(Column.class);
				sds = sds.select(cols);
			}

			List<JarveyColumn> columns = sds.getJarveySchema().getColumnAll();
			boolean[] mask = new boolean[columns.size()];
			Arrays.fill(mask, true);
			if ( !m_displayGeom ) {
				FStream.from(columns)
						.zipWithIndex()
						.filter(t -> t._1.getJarveyDataType().isGeometryType())
						.forEach(t -> mask[t._2] = false);
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
			SpatialDataset sds = jarvey.read().dataset(m_dsId);
			
			JarveySchema jschema = jarvey.loadJarveySchema(m_dsId);
			if ( jschema != null ) {
				System.out.println("COUNT         : " + sds.count());
				
				long totalSize = jarvey.getDatasetSize(m_dsId);
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
				
				long totalSize = jarvey.getDatasetSize(m_dsId);
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
	
	@Command(name="cluster", description="cluster dataset")
	public static class Cluster extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;
		
		@Parameters(paramLabel="output_dataset", index="1", arity="0..1",
					description={"output dataset id"})
		private String m_outDsId = null;

		@Option(names= {"--ref_dataset"}, paramLabel="dataset_id", description="reference dataset id")
		private void setReferenceDataset(String dsId) {
			m_refDsId = dsId;
		}
		private String m_refDsId = null;

		@Option(names= {"--sample_ratio"}, paramLabel="ratio", description="sampling ratio (default: 0.1)")
		private void setSampleRatio(double ratio) {
			m_sampleRatio = ratio;
		}
		private double m_sampleRatio = -1;

		@Option(names= {"--cluster_size"}, paramLabel="size", description="cluster size (default: '128mb')")
		private void setClusterSize(String sizeStr) {
			m_clusterSize = UnitUtils.parseByteSize(sizeStr);
		}
		private long m_clusterSize = -1;
	
		@Option(names={"-f", "--force"}, description="force to create clustered dataset")
		private boolean m_force;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			ClusterDatasetOptions opts = ClusterDatasetOptions.create()
															.force(m_force);
			if ( m_refDsId != null ) {
				JarveySchema jschema = jarvey.loadJarveySchema(m_refDsId);
				opts = opts.candidateQuadIds(jschema.getQuadIds());
			}
			if ( m_clusterSize > 0 ) {
				opts = opts.clusterSizeHint(m_clusterSize);
			}
			if ( m_sampleRatio > 0 ) {
				opts = opts.sampleRatio(m_sampleRatio);
			}
			String outDsId = (m_outDsId != null) ? m_outDsId : m_dsId + "_clustered";
			
			ClusterDataset cluster = new ClusterDataset(jarvey, m_dsId, outDsId, opts);
			cluster.call();
			
			System.out.printf("clustered: dataset=%s elapsed=%s%n",
								m_dsId, watch.getElapsedMillisString());
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
			HdfsPath srcDsPath = jarvey.getHdfsPath(m_src);
			HdfsPath destDsPath = jarvey.getHdfsPath(m_dest);
			srcDsPath.moveTo(destDsPath);
		}
	}

	@Command(name="set_sdsinfo", description="set SpatialDataset information")
	public static class SetSpatialDatasetInfo extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			SpatialDataset sds = jarvey.read().dataset(m_dsId);
			GeometryColumnInfo gcInfo = sds.getDefaultGeometryColumnInfo();
			StructType schema = sds.schema();
			
//			SpatialDatasetInfo sdInfo = new SpatialDatasetInfo(DatasetType.CSV, gcInfo, schema);
//			jarvey.saveSpatialDatasetInfo(m_dsId, sdInfo);
		}
	}

////	@Command(name="count", description="count records of the dataset")
////	public static class Count extends PicocliSubCommand<MarmotRuntime> {
////		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
////		private String m_dsId;
////
////		@Option(names="-mappers", paramLabel="count", description="mapper count")
////		private void setMapperCount(int count) {
////			m_mapperCount = FOption.of(count);
////		}
////		private FOption<Integer> m_mapperCount = FOption.empty();
////		
////		@Option(names={"-v", "-verbose"}, description="verbose")
////		private boolean m_verbose = false;
////
////		@Override
////		public void run(MarmotRuntime initialContext) throws Exception {
////			StopWatch watch = StopWatch.start();
////			
////			LoadOptions opts = LoadOptions.DEFAULT;
////			if ( m_mapperCount.isPresent() ) {
////				int cnt = m_mapperCount.getUnchecked();
////				opts = (cnt > 0) ? LoadOptions.MAPPERS(cnt) :LoadOptions.FIXED_MAPPERS;
////			}
////			Plan plan = Plan.builder("count records")
////								.load(m_dsId, opts)
////								.aggregate(AggregateFunction.COUNT())
////								.build();
////			long cnt = initialContext.executeToLong(plan).get();
////			watch.stop();
////			
////			if ( m_verbose ) {
////				System.out.printf("count=%d, elapsed=%s%n", cnt, watch.getElapsedMillisString());
////			}
////			else {
////				System.out.println(cnt);
////			}
////		}
////	}

	@Command(name="delete", description="delete the dataset(s)")
	public static class Delete extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			HdfsPath dsPath = jarvey.getHdfsPath(m_dsId);
			dsPath.delete();
		}
	}

	@Command(name="cluster_info", description="display cluster infomations")
	public static class ClusterInfo extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
		private String m_dsId;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			JarveySchema jschema = jarvey.loadJarveySchema(m_dsId);
			
			HdfsPath dsPath = jarvey.getHdfsPath(m_dsId);
			Long[] qids = jschema.getQuadIds();
			for ( int i =0; i < qids.length; ++i ) {
				String clusterFileName = String.format("%s=%d", SpatialDataset.CLUSTER_ID, qids[i]);
				HdfsPath clusterPath = dsPath.child(clusterFileName);
				long clusterSize = clusterPath.walkRegularFileTree()
											.mapOrThrow(HdfsPath::getLength)
											.mapToLong(v -> v)
											.sum();
				System.out.printf("qkey: %-17s(%8d): %8s\n", MapTile.toQuadKey(qids[i]), qids[i],
															UnitUtils.toByteSizeString(clusterSize));
			}
			System.out.printf("total nclusters: %d\n", qids.length);
		}
	}

////	@Command(name="attach_geometry", description="attach geometry data into the dataset")
////	public static class AttachGeometry extends PicocliSubCommand<MarmotRuntime> {
////		@Parameters(paramLabel="id", index="0", arity="1..1", description={"dataset id"})
////		private String m_dsId;
////
////		@Parameters(paramLabel="geometry_dataset", index="1", arity="1..1",
////					description={"geometry dataset id"})
////		private String m_geomDsId;
////
////		@Parameters(paramLabel="output_dataset", index="2", arity="1..1",
////					description={"output dataset id"})
////		private String m_outDsId;
////		
////		@Option(names={"-ref_col"}, paramLabel="column name", required=true,
////				description={"reference column in the dataset"})
////		private String m_refCol;
////		
////		@Option(names={"-key_col"}, paramLabel="column name", required=true,
////				description={"key column in the geometry dataset"})
////		private String m_keyCol;
////		
////		@Option(names={"-geom_col"}, paramLabel="column name", required=false,
////				description={"output geometry column name"})
////		private String m_geomCol = null;
////		
////		@Option(names={"-workers"}, paramLabel="worker count", required=false,
////				description={"join worker count"})
////		private FOption<Integer> m_nworkers = FOption.empty();
////
////		@Override
////		public void run(MarmotRuntime initialContext) throws Exception {
////			DataSet geomDs = initialContext.getDataSet(m_geomDsId);
////			if ( !geomDs.hasGeometryColumn() ) {
////				System.err.println("Geometry dataset does not have default Geometry column: "
////									+ "id=" + m_geomDsId);
////				System.exit(-1);
////			}
////			GeometryColumnInfo gcInfo = geomDs.getGeometryColumnInfo();
////
////			String outputGeomCol = (m_geomCol != null) ? m_geomCol : gcInfo.name();
////			JoinOptions opts = JoinOptions.INNER_JOIN(m_nworkers);
////
////			GeometryColumnInfo outGcInfo = new GeometryColumnInfo(outputGeomCol, gcInfo.srid());
////			String outputCols = String.format("param.%s as %s,*", gcInfo.name(), outputGeomCol);
////			Plan plan = Plan.builder("tag_geometry")
////									.load(m_dsId)
////									.hashJoin(m_refCol, m_geomDsId, m_keyCol, outputCols, opts)
////									.store(m_outDsId, FORCE(outGcInfo))
////									.build();
////			initialContext.execute(plan);
////		}
////	}

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
		private String m_start;
		
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

			int srid = m_shpParams.srid().getOrElse(ShapefileDataSets.loadSrid(new File(m_start)));
			GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", srid);
			
			Dataset<Row> df = reader.shapefile(m_start);
			SpatialDataset sds = jarvey_functions.spatial(jarvey, df, gcInfo);
			
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

//	@Command(name="geojson", description="import geojson file into the dataset")
//	public static class ImportGeoJsonCmd extends PicocliSubCommand<MarmotRuntime> {
//		@Mixin private GeoJsonParameters m_gjsonParams;
//		
//		@Parameters(paramLabel="path", index="0", arity="1..1",
//					description={"path to the target geojson files (or directories)"})
//		private File m_start;
//		
//		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
//					description={"dataset id to import onto"})
//		private String m_dsId;
//
//		@Option(names={"-f"}, description="force to create a dataset")
//		private boolean m_force = false;
//
//		@Override
//		public void run(MarmotRuntime marmot) throws Exception {
//			StopWatch watch = StopWatch.start();
//			
//			RecordReader reader = GeoJsonRecordReader.from(m_start, m_gjsonParams.charset());
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
//
////	@Command(name="jdbc", description="import a JDBC-connected table into a dataset")
////	public static class ImportJdbcCmd extends PicocliSubCommand<MarmotRuntime> {
////		@Mixin private LoadJdbcParameters m_jdbcParams;
////		@Mixin private ImportParameters m_importParams;
////
////		@Parameters(paramLabel="table_name", index="0", arity="1..1",
////					description={"JDBC table name"})
////		private String m_tableName;
////		
////		@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
////				description={"dataset id to import onto"})
////		public void setDataSetId(String id) {
////			Utilities.checkNotNullArgument(id, "dataset id is null");
////			m_importParams.setDataSetId(id);
////		}
////
////		@Override
////		public void run(MarmotRuntime initialContext) throws Exception {
////			StopWatch watch = StopWatch.start();
////			
////			ImportJdbcTable importFile = ImportJdbcTable.from(m_tableName, m_jdbcParams,
////																m_importParams);
////			importFile.getProgressObservable()
////						.subscribe(report -> {
////							double velo = report / watch.getElapsedInFloatingSeconds();
////							System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
////											report, watch.getElapsedMillisString(), velo);
////						});
////			long count = importFile.run(initialContext);
////			
////			double velo = count / watch.getElapsedInFloatingSeconds();
////			System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
////								m_importParams.getDataSetId(), count, watch.getElapsedMillisString(), velo);
////		}
////	}
	
	@Command(name="export",
			subcommands= {
//				ExportCsv.class,
				ExportShapefile.class,
//				ExportGeoJson.class,
//				ExportJdbcTable.class,
			},
			description="export a dataset")
	public static class Export extends PicocliSubCommand<JarveySession> {
		@Override
		public void run(JarveySession initialContext) throws Exception { }
	}
	
////	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
////
////	@Command(name="csv", description="export a dataset in CSV format")
////	public static class ExportCsv extends PicocliSubCommand<MarmotRuntime> {
////		@Parameters(paramLabel="dataset_id", index="0", arity="1..1",
////					description={"dataset id to export"})
////		private String m_dsId;
////
////		@Parameters(paramLabel="file_path", index="1", arity="0..1",
////					description={"file path for exported CSV file"})
////		private String m_output;
////
////		@Mixin private CsvParameters m_csvParams;
////		
////		@Option(names={"-f"}, description="delete the file if it exists already")
////		private boolean m_force;
////
////		@Override
////		public void run(MarmotRuntime initialContext) throws Exception {
////			m_csvParams.charset().ifAbsent(() -> m_csvParams.charset(DEFAULT_CHARSET));
////			
////			File outFile = new File(m_output);
////			if ( m_force && m_output != null && outFile.exists() ) {
////				FileUtils.forceDelete(outFile);
////			}
////			
////			FOption<String> output = FOption.ofNullable(m_output);
////			BufferedWriter writer = ExternIoUtils.toWriter(output, m_csvParams.charset().get());
////			new ExportAsCsv(m_dsId, m_csvParams).run(initialContext, writer);
////		}
////	}

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
			SpatialDataset sds = jarvey.read().dataset(m_dsId);
			
			int srid = sds.getDefaultGeometryColumnInfo().srid();
			Charset charset = m_shpParams.charset();

			int shpSrid = m_shpParams.shpSrid().getOrElse(0);
			if ( shpSrid != 0 && shpSrid != srid ) {
				sds = sds.transform(shpSrid);
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
}
