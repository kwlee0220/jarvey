package jarvey.appls;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Test {
	public static final void main(String... args) throws Exception {
		double[] array = new double[1024 * 1024];
		for ( int i =0; i < array.length; ++i ) {
			array[i] = (double)i / 1024;
		}
		
		ByteBuffer bbuf = ByteBuffer.allocate(1024 * 1024 * 8);
		DoubleBuffer dbuf = bbuf.asDoubleBuffer();
		dbuf.put(array);
		
		byte[] decompressed = bbuf.array();
		byte[] compressed = IOUtils.compress(bbuf.array());
		System.out.printf("%d, %d, %.3f%n", decompressed.length, compressed.length,
						(double)compressed.length/decompressed.length);
	}
}
