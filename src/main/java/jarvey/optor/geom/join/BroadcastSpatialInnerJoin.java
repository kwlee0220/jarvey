package jarvey.optor.geom.join;

import java.util.List;
import java.util.Set;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import jarvey.support.RecordLite;
import jarvey.support.colexpr.JoinColumnSelector;
import jarvey.support.colexpr.SelectedColumnInfo;
import jarvey.type.JarveySchema;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BroadcastSpatialInnerJoin extends BroadcastSpatialJoin {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(BroadcastSpatialInnerJoin.class);
	
	private JoinColumnSelector m_selector;
	
	public BroadcastSpatialInnerJoin(JarveySchema paramSchema, Broadcast<List<Row>>
										params, SpatialJoinOptions opts) {
		super(paramSchema, params, opts);
		
		setLogger(s_logger);
	}

	@Override
	protected JarveySchema initializeJoin(JarveySchema inputSchema) {
		m_selector = m_opts.outputColumns()
							.map(expr -> new JoinColumnSelector(inputSchema, m_paramSchema, expr))
							.getOrNull();
		if ( m_selector == null ) {
			throw new IllegalArgumentException("output columns not specified");
		}
		
		Set<String> outputCols = Sets.newHashSet();
		m_selector.findColumnInfo("left", getInputGeometryColumnInfo().getName())
					.map(SelectedColumnInfo::getOutputColumnName)
					.ifPresent(outputCols::add);
		m_selector.findColumnInfo("right", m_paramGeomColInfo.getName())
					.map(SelectedColumnInfo::getOutputColumnName)
					.ifPresent(outputCols::add);
		return m_selector.getOutputJarveySchema();
	}

	protected FStream<RecordLite> joinWithMatchingInners(EnvelopedRecordLite outer,
														FStream<EnvelopedRecordLite> inners) {
		return inners.map(inner -> m_selector.select(getInputSchema(), outer.getRecord(),
														m_paramSchema, inner.getRecord()));
	}
	
	@Override
	public String toString() {
		return String.format("%s", getClass().getSimpleName());
	}
}
