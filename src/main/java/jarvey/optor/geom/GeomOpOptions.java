package jarvey.optor.geom;

import java.io.Serializable;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeomOpOptions implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final GeomOpOptions DEFAULT = new GeomOpOptions(null, false);
	
	private String m_outColumn;
	private boolean m_throwOpError;
	
	private GeomOpOptions(String outputColumns, boolean throwOpError) {
		m_outColumn = outputColumns;
		m_throwOpError = throwOpError;
	}
	
	public static GeomOpOptions OUTPUT(String outCol) {
		return new GeomOpOptions(outCol, false);
	}
	
	public FOption<String> outputColumn() {
		return FOption.ofNullable(m_outColumn);
	}
	
	public GeomOpOptions outputColumn(String outCol) {
		return new GeomOpOptions(outCol, m_throwOpError);
	}
	
	public boolean throwOpError() {
		return m_throwOpError;
	}
	
	public GeomOpOptions throwOpError(boolean flag) {
		return new GeomOpOptions(m_outColumn, flag);
	}
	
	@Override
	public String toString() {
		String outStr = m_outColumn != null ? "output=" + m_outColumn : "";
		return String.format("%s, throw_error=%s", outStr, m_throwOpError);
	}
}
