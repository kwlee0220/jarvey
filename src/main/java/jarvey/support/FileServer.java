package jarvey.support;

import java.io.IOException;
import java.io.InputStream;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface FileServer {
	public InputStream readFile(String path) throws JarveyFileNotFoundException;
	public long writeFile(String path, InputStream is) throws IOException;
	public boolean deleteFile(String path);
	
	public FStream<String> walkRegularFileTree(String start);
}
