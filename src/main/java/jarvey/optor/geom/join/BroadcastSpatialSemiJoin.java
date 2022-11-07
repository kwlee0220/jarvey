package jarvey.optor.geom.join;

import java.util.List;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.support.RecordLite;
import jarvey.type.JarveySchema;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BroadcastSpatialSemiJoin extends BroadcastSpatialJoin {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(BroadcastArcClip.class);
	
	public BroadcastSpatialSemiJoin(JarveySchema paramSchema, Broadcast<List<Row>> params,
									SpatialJoinOptions opts) {
		super(paramSchema, params, opts);
		
		setLogger(s_logger);
	}

	@Override
	protected JarveySchema initializeJoin(JarveySchema inputSchema) {
		return getInputSchema();
	}

	@Override
	protected FStream<RecordLite> joinWithMatchingInners(EnvelopedRecordLite outer,
														FStream<EnvelopedRecordLite> inners) {
		boolean matched = inners.exists();
		matched = m_opts.isNegated() ? !matched : matched;
		return matched ? FStream.of(outer.getRecord()) : FStream.empty();
	}
	
	@Override
	public String toString() {
		return String.format("%s", getClass().getSimpleName());
	}
}
