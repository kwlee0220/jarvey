package jarvey.quadtree;

import org.locationtech.jts.geom.Envelope;

import utils.stream.FStream;
import utils.stream.IntFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class PointerGroup implements Enveloped {
	private final Envelope m_envl;
	private final int[] m_indexes;
	
	PointerGroup(Envelope envl, int[] indexes) {
		m_envl = envl;
		m_indexes = indexes;
	}

	@Override
	public Envelope getEnvelope84() {
		return m_envl;
	}
	
	FStream<Pointer> stream() {
		return IntFStream.of(m_indexes)
					.mapToObj(index -> new Pointer(m_envl, index));
	}
}