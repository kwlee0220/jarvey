package jarvey.optor;

import java.util.Iterator;

import jarvey.support.RecordLite;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RecordLevelFunction extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;

	abstract protected RecordLite mapRecord(RecordLite input);
	
	protected RecordLevelFunction() {
		super(true);
	}

	@Override
	protected final Iterator<RecordLite> mapPartition(int partIdx, Iterator<RecordLite> iter) {
		MapIterator<RecordLite,RecordLite> outIter = new MapIterator<RecordLite,RecordLite>(iter) {
			@Override
			protected RecordLite apply(RecordLite input) {
				return mapRecord(input);
			}
		};
		outIter.setLogger(getLogger());
		outIter.start();
		
		return outIter;
	}
}
