package jarvey.datasource;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(description="CSV Parameters")
public class CsvParameters {
	private Map<String,String> m_options = Maps.newHashMap();
	private @Nullable String m_pointCols = null;
	
	public Map<String,String> options() {
		return m_options;
	}
	
	public static CsvParameters create() {
		return new CsvParameters();
	}

	@Option(names={"--delimeter"}, paramLabel="char", description={"delimiter for CSV file"})
	public CsvParameters delimiter(String delim) {
		m_options.put("delimiter", delim);
		return this;
	}

	@Option(names={"--quote"}, paramLabel="char", description={"quote character for CSV file"})
	public CsvParameters quote(String quote) {
		m_options.put("quote", quote);
		return this;
	}

	@Option(names={"--escape"}, paramLabel="char", description={"quote escape character for CSV file"})
	public CsvParameters escape(String escape) {
		m_options.put("escape", escape);
		return this;
	}

	@Option(names={"--encoding"}, paramLabel="encoding", description={"Character encoding of the target CSV file"})
	public CsvParameters encoding(String encoding) {
		m_options.put("encoding", encoding);
		return this;
	}

	@Option(names={"--header"}, description="consider the first line as header")
	public CsvParameters header(boolean flag) {
		m_options.put("header", "" + flag);
		return this;
	}

	@Option(names={"--nullValue"}, paramLabel="string", description="null value for column")
	public CsvParameters nullValue(String str) {
		m_options.put("nullValue", str);
		return this;
	}

	@Option(names={"--lineSep"}, paramLabel="separator", description="line separator")
	public CsvParameters lineSep(String str) {
		m_options.put("lineSep", str);
		return this;
	}

	@Option(names={"--compression"}, paramLabel="codec", description="compression codec")
	public CsvParameters compression(String codec) {
		m_options.put("compression", codec);
		return this;
	}

	@Option(names={"--pointCols"}, paramLabel="codec", description="compression codec")
	public CsvParameters pointCols(String cols) {
		m_pointCols = cols;
		return this;
	}
	
	public String pointCols() {
		return m_pointCols;
	}
}
