package jarvey.join;

import java.util.List;

import org.locationtech.jts.geom.Envelope;

import com.google.common.collect.Lists;

import jarvey.quadtree.Enveloped;
import jarvey.quadtree.Pointer;
import jarvey.quadtree.PointerPartition;
import jarvey.quadtree.QuadTree;
import jarvey.support.MapTile;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QTreeLookupTable<T extends Enveloped> implements SpatialLookupTable<T> {
	private final QuadTree<Pointer,PointerPartition> m_qtree;
	private final List<T> m_rows;
	
	public QTreeLookupTable(long quadId, FStream<T> rows) {
		String quadKey = MapTile.toQuadKey(quadId);
		if ( quadKey.equals(MapTile.QKEY_OUTLIER) ) {
			quadKey = "";
		}

		m_qtree = new QuadTree<>(quadKey, qkey->new PointerPartition());
		m_rows = Lists.newArrayList();
		rows.forEach(rx -> {
			Envelope envl = rx.getEnvelope84();
			if ( envl != null ) {
				m_rows.add(rx);
				m_qtree.insert(new Pointer(envl, m_rows.size()-1));
			}
		});
	}

	public FStream<T> query(Envelope range84, boolean dropGuests) {
		return m_qtree.query(SpatialRelation.INTERSECTS, range84)
						.distinct()
						.map(ptr -> m_rows.get(ptr.index()));
	}
}