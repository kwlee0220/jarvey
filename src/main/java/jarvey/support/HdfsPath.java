package jarvey.support;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class HdfsPath implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(HdfsPath.class);
	
	private transient Configuration m_conf;
	private transient Path m_path;
	private transient FileSystem m_fs;
	
	public static HdfsPath of(Configuration conf, Path path) {
		return new HdfsPath(conf, null, path);
	}
	
	public static HdfsPath of(FileSystem fs, Path path) {
		return new HdfsPath(null, fs, path);
	}
	
	public static HdfsPath of(Configuration conf, FileStatus fstat) {
		return new HdfsPath(conf, null, fstat);
	}
	
	public static HdfsPath local(File rootFile) {
		return of(new Configuration(), new Path(rootFile.getAbsolutePath()));
	}
	
	private HdfsPath(Configuration conf, FileSystem fs, Path path) {
		Utilities.checkArgument(conf != null || fs != null, "conf != null || fs != null");
		Utilities.checkNotNullArgument(path, "path is null");
		
		m_conf = conf != null ? conf : fs.getConf();
		m_fs = fs;
		m_path = path;
	}
	
	private HdfsPath(Configuration conf, FileSystem fs, FileStatus fstat) {
		Utilities.checkArgument(conf != null || fs != null, "conf != null || fs != null");
		Utilities.checkNotNullArgument(fstat, "FileStatus is null");

		m_conf = conf != null ? conf : fs.getConf();
		m_fs = fs;
		m_path = fstat.getPath();
	}
	
	public Configuration getConf() {
		return m_conf;
	}
	
	public Path getPath() {
		return m_path;
	}
	
	public HdfsPath getAbsolutePath() throws IOException {
		return m_path.isAbsolute() ? this : HdfsPath.of(m_conf, getFileStatus().getPath());
	}
	
	public String getName() {
		return m_path.getName();
	}
	
	public long getLength() throws IOException {
		return getFileStatus().getLen();
	}
	
	public long getTotalLength() throws IOException {
		return walkRegularFileTree().mapOrIgnore(HdfsPath::getLength).mapToLong(v -> v).sum();
	}
	
	public FileSystem getFileSystem() {
		if ( m_fs == null ) {
			try {
				m_fs = FileSystem.get(m_conf);
			}
			catch ( Exception e ) {
				throw new JarveyFileException("" + e);
			}
		}
		
		return m_fs;
	}

	public FileStatus getFileStatus() throws IOException {
		return getFileSystem().getFileStatus(m_path);
	}
	
	public FOption<HdfsPath> getParent() {
		Path parentPath = m_path.getParent();
		return (parentPath != null)
				? FOption.of(new HdfsPath(m_conf, m_fs, parentPath))
				: FOption.empty();
	}
	
	public HdfsPath child(String name) {
		Utilities.checkNotNullArgument(name, "name is null");
		
		return new HdfsPath(m_conf, m_fs, new Path(m_path, name));
	}

	public boolean exists() {
		try {
			return getFileSystem().exists(m_path);
		}
		catch ( AccessControlException e ) {
			return false;
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}

	public boolean isDirectory() throws IOException {
		return getFileStatus().isDirectory();
	}

	public boolean isFile() throws IOException {
		return getFileStatus().isFile();
	}

	public boolean isRegularFile() throws IOException {
		if ( isFile() ) {
			String fname = getName();
			if ( fname.startsWith("_") || fname.startsWith(".") ) {
				return false;
			}
		}
		
		return false;
	}

	public boolean isRegular() {
		String fname = getName();
		return !(fname.startsWith("_") || fname.startsWith("."));
	}
	
	public boolean makeParentDirectory() {
		FOption<HdfsPath> oparent = getParent();
		if ( oparent.isPresent() ) {
			HdfsPath parent = oparent.get();
			if ( !parent.exists() ) {
				if ( !parent.makeParentDirectory() ) {
					return false;
				}
				return parent.mkdir();
			}
		}
		
		return true;
	}
	
	public FSDataOutputStream create() {
		try {
			makeParentDirectory();
			return getFileSystem().create(m_path);
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}
	
	public FSDataOutputStream create(boolean overwrite, long blockSize) {
		int bufferSz = m_conf.getInt(
			                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
			                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
		try {
			makeParentDirectory();
			return getFileSystem().create(m_path, overwrite, bufferSz,
										m_fs.getDefaultReplication(m_path), blockSize);
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}
	
	public FSDataOutputStream append() {
		try {
			return getFileSystem().append(m_path);
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}
	
	public FSDataInputStream open() {
		try {
			return getFileSystem().open(m_path);
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}
	
	/**
	 * ????????? ???????????? HDFS ????????? ????????????.
	 * ?????? ????????? ?????? ????????? ????????? ????????? {@code false}??? ????????????.
	 * 
	 * @return	?????? ?????? ??????.
	 */
	public boolean delete() {
		try {
			FileSystem fs = getFileSystem();
			
			if ( !exists() ) {
				return true;
			}
			
			return fs.delete(m_path, true);
		}
		catch ( IOException e ) {
			s_logger.warn("fails delete file: " + this, e);
			return false;
		}
	}
	
	public boolean deleteUpward() {
		if ( !delete() ) {
			return false;
		}

		return getParent().map(HdfsPath::deleteIfEmptyDirectory)
							.getOrElse(true);
	}
	
	private boolean deleteIfEmptyDirectory() {
		try {
			if ( getFileSystem().listStatus(m_path).length == 0 ) {
				if ( !delete() ) {
					return false;
				}
				
				return getParent().map(HdfsPath::deleteIfEmptyDirectory)
									.getOrElse(true);
			}
			else {
				return true;
			}
		}
		catch ( IOException e ) {
			return false;
		}
	}
	
	public boolean mkdir() {
		if ( exists() ) {
			throw new JarveyFileException("already exists: path=" + m_path);
		}
		
		try {
			FOption<HdfsPath> oparent = getParent();
			if ( oparent.isPresent() ) {
				HdfsPath parent = oparent.getUnchecked();
				
				if ( !parent.exists() ) {
					if ( !parent.mkdir() ) {
						return false;
					}
				}
				else if ( !parent.isDirectory() ) {
					throw new JarveyFileException("the parent file is not a directory");
				}
			}
			
			return getFileSystem().mkdirs(m_path);
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}
	
	public void moveTo(HdfsPath dst) {
		Utilities.checkNotNullArgument(dst, "dst is null");
		
		if ( !exists() ) {
			throw new JarveyFileException("source file not found: path=" + m_path);
		}
		
		if ( dst.exists() ) {
			throw new JarveyFileException("destination exists: path=" + dst);
		}

		try {
			FOption<HdfsPath> oparent = dst.getParent();
			if ( oparent.isPresent() ) {
				HdfsPath parent = oparent.getUnchecked();
				if ( !parent.exists() ) {
					if ( !parent.mkdir() ) {
						throw new JarveyFileException("fails to create destination's parent directory");
					}
				}
				else if ( !parent.isDirectory() ) {
					throw new JarveyFileException("destination's parent is not a directory: path=" + parent);
				}
			}
			
			if ( !getFileSystem().rename(m_path, dst.m_path) ) {
				throw new JarveyFileException("fails to rename to " + dst);
			}

			// ?????? ?????? ???, ??????????????? ?????? ?????? ?????? ??????????????? ??????????????? ????????????.
			getParent().ifPresent(HdfsPath::deleteIfEmptyDirectory);
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}
	
	public FStream<HdfsPath> streamChildFiles() {
		try {
			return FStream.of(getFileSystem().listStatus(m_path))
							.map(s -> HdfsPath.of(m_conf, s));
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}

	/**
	 * ????????? ????????? ???????????? ??????????????? ????????? ?????? ?????? ????????? ?????? ????????? ???????????? ????????????.
	 * ????????? ???????????? ?????? ??????????????? (???????????????) ????????? ?????? ?????? ????????? ????????? ????????? ??? ??????.
	 * ?????? ????????? ????????? ?????? ????????? ????????? ????????? ?????? ?????? ????????? ?????? ???????????? ???????????? ???????????? ????????????.
	 * 
	 * @return	 ????????? ?????? ????????? ?????????
	 * @throws IOException 
	 */
	public FStream<HdfsPath> walkRegularFileTree() throws IOException {
		return walkTree(getFileSystem(), m_path)
					.filter(IS_REGULAR_FILE)
					.map(fstat -> HdfsPath.of(m_conf, fstat));
	}

	/**
	 * ????????? ????????? ???????????? ??????????????? ????????? ?????? ????????? ?????? ????????? ???????????? ????????????.
	 * ????????? ???????????? ?????? ??????????????? (???????????????) ????????? ?????? ????????? ????????? ????????? ??? ??????.
	 * ?????? ????????? ????????? ????????? ????????? ????????? ?????? ????????? ?????? ???????????? ???????????? ???????????? ????????????.
	 * 
	 * @param includeCurrent	???????????? ??? ?????? ?????? ??????
	 * @return	 ????????? ?????? ????????? ?????????
	 * @throws IOException 
	 */
	public FStream<HdfsPath> walkTree(boolean includeCurrent) throws IOException {
		FStream<FileStatus> strm = walkTree(getFileSystem(), m_path);
		if ( !includeCurrent ) {
			strm = strm.drop(1);
		}
		return strm.map(fstat -> HdfsPath.of(m_conf, fstat));
	}
	
	public static FStream<HdfsPath> walkRegularFileTree(Configuration conf, List<Path> starts) {
		return walkRegularFileTree(getFileSystem(conf), starts);
	}
	
	public static FStream<HdfsPath> walkRegularFileTree(FileSystem fs, Path... starts) {
		return walkRegularFileTree(fs, Arrays.asList(starts));
	}
	
	public static FStream<HdfsPath> walkRegularFileTree(FileSystem fs, List<Path> starts) {
		return FStream.from(starts)
						.map(path -> HdfsPath.of(fs, path))
						.flatMap(path -> {
							try {
								return path.walkRegularFileTree();
							}
							catch ( Exception e ) {
								s_logger.warn("fails to traverse: file=" + path + ", cause=" + e);
								return FStream.empty();
							}
						});
	}
	
	public static FStream<HdfsPath> walkRegularFileTree(List<HdfsPath> starts) {
		return FStream.from(starts)
						.flatMap(path -> {
							try {
								return path.walkRegularFileTree();
							}
							catch ( Exception e ) {
								s_logger.warn("fails to traverse: file=" + path + ", cause=" + e);
								return FStream.empty();
							}
						});
	}

	public static Predicate<FileStatus> IS_REGULAR_FILE = (fstat) -> {
		if ( fstat.isDirectory() ) {
			return false;
		}
		String fname = fstat.getPath().getName();
		if ( fname.startsWith("_") || fname.startsWith(".") ) {
			return false;
		}
		return true;
	};

	public static FileSystem getLocalFileSystem() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", "file:///");
			
			return FileSystem.get(conf);
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}

	public static FileSystem getFileSystem(Configuration conf) {
		try {
			return FileSystem.get(conf);
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}

	public static Configuration getLocalFsConf() {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		
		return conf;
	}
	
	@Override
	public String toString() {
		return m_path.toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		else if ( o == null || getClass() != o.getClass() ) {
			return false;
		}
		
		HdfsPath other = (HdfsPath)o;
		return m_path.equals(other.m_path);
	}
	
	@Override
	public int hashCode() {
		return m_path.hashCode();
	}
	
	private void writeObject(ObjectOutputStream os) throws IOException {
		os.defaultWriteObject();
		
		m_conf.write(os);
		os.writeUTF(m_path.toString());
	}
	
	private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
		is.defaultReadObject();
		
		m_conf = new Configuration();
		m_conf.readFields(is);
		
		m_path = new Path(is.readUTF());
	}
	
	private static FStream<FileStatus> walkTree(FileSystem fs, Path start) {
		try {
			FileStatus current = fs.getFileStatus(start);
			if ( current.isDirectory() ) {
				FStream<FileStatus> children = FStream.of(fs.listStatus(start))
														.flatMap(fstat -> {
															if ( fstat.isDirectory() ) {
																Path path = fstat.getPath();
																return walkTree(fs, path);
															}
															else {
																return FStream.of(fstat);
															}
														});
				return FStream.concat(FStream.of(current), children);
			}
			else {
				return FStream.of(current);
			}
		}
		catch ( FileNotFoundException e ) {
			throw new JarveyFileNotFoundException("" + start);
		}
		catch ( IOException e ) {
			throw new JarveyFileException(e);
		}
	}
}
