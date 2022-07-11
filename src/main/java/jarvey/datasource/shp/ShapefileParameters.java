package jarvey.datasource.shp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nullable;

import picocli.CommandLine.Option;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	@Nullable private Charset m_charset = DEFAULT_CHARSET;
	@Nullable private FOption<Integer> m_srid = FOption.empty();
	
	public static ShapefileParameters create() {
		return new ShapefileParameters();
	}
	
	public Charset charset() {
		return m_charset;
	}

	@Option(names={"--charset"}, paramLabel="charset",
			description={"Character encoding of the target shapefile file"})
	public ShapefileParameters charset(String charset) {
		Utilities.checkNotNullArgument(charset);
		
		m_charset = Charset.forName(charset);
		return this;
	}
	
	public ShapefileParameters charset(Charset charset) {
		Utilities.checkNotNullArgument(charset);
		
		m_charset = charset;
		return this;
	}
	
	@Option(names= {"--srid"}, paramLabel="EPSG-code", description="shapefile SRID")
	public ShapefileParameters srid(int srid) {
		m_srid = FOption.of(srid);
		return this;
	}
	
	public FOption<Integer> srid() {
		return m_srid;
	}
	
	@Override
	public String toString() {
		String srcSrid = srid().map(s -> String.format(", srid=%s", s))
								.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}