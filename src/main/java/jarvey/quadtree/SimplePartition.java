package jarvey.quadtree;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SimplePartition<T extends Enveloped> extends AbstractPartition<T> {
	private final int m_maxCount;
	
	public SimplePartition(int maxCount) {
		m_maxCount = maxCount;
	}

	@Override
	protected boolean mayOverflow(T value) {
		return size() + 1 > m_maxCount;
	}

	@Override
	public boolean tryExpand() {
		return false;
	}
	
	@Override
	public String toString() {
		return String.format("%d/%d", size(), m_maxCount);
	}
}
