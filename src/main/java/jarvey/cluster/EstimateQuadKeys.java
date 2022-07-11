package jarvey.cluster;

import static jarvey.jarvey_functions.ST_IsValidEnvelope;
import static org.apache.spark.sql.functions.col;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.locationtech.jts.geom.Envelope;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import avro.shaded.com.google.common.collect.Sets;
import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.support.MapTile;
import jarvey.type.EnvelopeValue;
import jarvey.type.GeometryColumnInfo;
import utils.func.Tuple;
import utils.stream.FStream;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class EstimateQuadKeys implements Callable<Map<String,Tuple<Integer,Float>>> {
	private static final double MB_FACTOR = 1024 * 1024.0;
	
	private final JarveySession m_jarvey;
	private final String m_dsId;
	private final EstimateQuadKeysOptions m_opts;
	
	private static final String ENVL4326 = SpatialDataset.ENVL4326;
	
	public EstimateQuadKeys(JarveySession jarvey, String dsId, EstimateQuadKeysOptions opts) {
		m_jarvey = jarvey;
		m_dsId = dsId;
		m_opts = opts;
	}

	@Override
	public Map<String,Tuple<Integer,Float>> call() {
		SpatialDataset sds = m_jarvey.read().dataset(m_dsId);
		
		long totalSize = m_jarvey.getDatasetSize(m_dsId);
		double ratio = (m_opts.clusterSizeHint()/MB_FACTOR) / (totalSize/MB_FACTOR);
		
		double sampleRatio = Math.min(1, Math.max(m_opts.sampleRatio(), ratio));
		SpatialDataset sampled = sample(m_jarvey, sds, sampleRatio).cache();
		long nsamples = sampled.count();

		int nRowsPerCluster = (int)Math.round(nsamples * ratio);
		int threshhold = (int)Math.round(nsamples * ratio * 0.85);
		Map<String,List<Integer>> clusters = estimateQuadKeys(sampled, threshhold);
		
		Map<String,Tuple<Integer,Float>> sampleHist = Maps.newHashMap();
		for ( Map.Entry<String, List<Integer>> ent: clusters.entrySet() ) {
			String qkey = ent.getKey();
			List<Integer> ptrs = ent.getValue();
			sampleHist.put(qkey, Tuple.of(ptrs.size(), (float)((double)ptrs.size()/nRowsPerCluster)));
		}
		
		return sampleHist;
	}
	
	private SpatialDataset sample(JarveySession jarvey, SpatialDataset sdf, double sampleRatio) {
		GeometryColumnInfo gcInfo = sdf.getDefaultGeometryColumnInfo();
		return sdf.sample(sampleRatio)
					.filter(col(gcInfo.name()).isNotNull())
					.box2d(ENVL4326)
					.transform_box(ENVL4326, 4326)
					.filter(ST_IsValidEnvelope(col(ENVL4326)))
					.select(ENVL4326);
	}
	
	private Map<String,List<Integer>> estimateQuadKeys(SpatialDataset sampleDf, int nRowsPerCluster) {
		List<Envelope> samples = FStream.from(sampleDf.toLocalIterator())
										.map(row -> EnvelopeValue.toEnvelope(row.getAs(0)))
										.toList();
		List<Integer> pointers = FStream.range(0, samples.size()).toList();
		Map<String,List<Integer>> clusters = Maps.newHashMap();
		
		String startQuadKey = "";
		List<Tuple<String,List<Integer>>> remains = Lists.newArrayList(Tuple.of(startQuadKey, pointers));
		while ( remains.size() > 0 ) {
			Tuple<String,List<Integer>> tup = remains.remove(0);
			
			Map<String,List<Integer>> tiles = split(tup._1, samples, tup._2);
			if ( tiles.size() == 1 ) {
				Map.Entry<String,List<Integer>> ent = tiles.entrySet().iterator().next();
				remains.add(Tuple.of(ent.getKey(), ent.getValue()));
			}
			else {
				for ( Map.Entry<String, List<Integer>> ent: tiles.entrySet() ) {
					String qkey = ent.getKey();
					List<Integer> ptrs = ent.getValue();
					
					if ( ent.getValue().size() <= nRowsPerCluster ) {
						List<Integer> prev = clusters.get(qkey);
						if ( prev != null ) {
							ptrs = Lists.newArrayList(Sets.union(Sets.newHashSet(prev), Sets.newHashSet(ptrs)));
						}
						clusters.put(qkey, ptrs);
					}
					else {
						remains.add(Tuple.of(qkey, ptrs));
					}
				}
			}
		}
		
		return clusters;
	}
	
	private static Map<String,List<Integer>> split(String parent, List<Envelope> samples,
													List<Integer> pointers) {
		List<MapTile> tiles = FStream.range(0, 4)
									.mapToObj(v -> parent + v.toString())
									.map(qkey -> MapTile.fromQuadKey(qkey))
									.toList();
		Map<String,List<Integer>> splits = Maps.newHashMap();
		for ( int idx: pointers ) {
			Envelope envl = samples.get(idx);
			for ( MapTile tile: tiles ) {
				if ( tile.contains(envl) ) {
					List<Integer> bucket = splits.computeIfAbsent(tile.getQuadKey(), t -> Lists.newArrayList());
					bucket.add(idx);
				}
			}
		}
		
		return splits;
	}
}
