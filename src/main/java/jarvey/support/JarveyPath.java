package jarvey.support;

import java.io.IOException;
import java.io.InputStream;

import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


public interface JarveyPath {
	public InputStream open();
	public InputStream create();

	public String getName();
	public long getLength();
	
	public FOption<JarveyPath> getParent();
	public JarveyPath child(String name);
	
	/**
	 * 경로에 해당하는 파일을 삭제한다.
	 * 여러 이유로 파일 삭제에 실패한 경우는 {@code false}를 반환한다.
	 * 
	 * @return	파일 삭제 여부.
	 */
	public boolean delete();
	
	public default boolean deleteIfEmptyDirectory() {
		if ( getLength() == 0 ) {
			if ( !delete() ) {
				return false;
			}
			
			return getParent().map(JarveyPath::deleteIfEmptyDirectory)
								.getOrElse(true);
		}
		else {
			return true;
		}
	}
	
	public boolean mkdir();
	
	public boolean renameTo(JarveyPath toPath);
	
	public boolean exists();

	public boolean isDirectory();
	public boolean isFile();

	public default boolean isRegular() {
		String fname = getName();
		return !(fname.startsWith("_") || fname.startsWith("."));
	}

	public default boolean isRegularFile() {
		return isFile() && isRegular();
	}
	public FStream<JarveyPath> walkTree(boolean includeCurrent);
	
	public default FStream<JarveyPath> walkRegularFileTree() throws IOException {
		return walkTree(true)
					.filter(JarveyPath::isRegularFile);
	}
	
	public default long getTotalLength() throws IOException {
		return walkRegularFileTree().mapOrIgnore(JarveyPath::getLength).mapToLong(v -> v).sum();
	}
	
	public default void moveTo(JarveyPath dst) {
		Utilities.checkNotNullArgument(dst, "dst is null");
		
		if ( !exists() ) {
			throw new JarveyFileException("source file not found: path=" + this);
		}
		
		if ( dst.exists() ) {
			throw new JarveyFileException("destination exists: path=" + dst);
		}

		FOption<JarveyPath> oparent = dst.getParent();
		if ( oparent.isPresent() ) {
			JarveyPath parent = oparent.getUnchecked();
			if ( !parent.exists() ) {
				if ( !parent.mkdir() ) {
					throw new JarveyFileException("fails to create destination's parent directory");
				}
			}
			else if ( !parent.isDirectory() ) {
				throw new JarveyFileException("destination's parent is not a directory: path=" + parent);
			}
		}
		
		if ( renameTo(dst) ) {
			throw new JarveyFileException("fails to rename to " + dst);
		}

		// 파일 이동 후, 디렉토리가 비게 되면 해당 디렉토리를 올라가면서 삭제한다.
		getParent().ifPresent(JarveyPath::deleteIfEmptyDirectory);
	}
}
