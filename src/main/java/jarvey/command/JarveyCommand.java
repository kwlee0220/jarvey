package jarvey.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import utils.PicocliCommand;
import utils.UsageHelp;
import utils.Utilities;
import utils.func.FOption;

import jarvey.JarveySession;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;
import picocli.CommandLine.Spec;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class JarveyCommand implements PicocliCommand<JarveySession> {
	private static final Logger s_logger = LoggerFactory.getLogger(JarveyCommand.class);
	private static final String ENVVAR_HOME = "JARVEY_HOME";
	
	@Spec protected CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Nullable private File m_homeDir = null;
	
	@Option(names={"-v"}, description={"verbose"})
	protected boolean m_verbose = false;
	
	@Option(names={"--db_root"}, paramLabel="path", description={"HDFS path prefix"})
	protected String m_dbRootPath = null;
	
	@Nullable private JarveySession m_jarvey;
	
	protected abstract void run(JarveySession marmot) throws Exception;

	@SuppressWarnings("deprecation")
	public static final void run(JarveyCommand cmd, String... args) throws Exception {
		new CommandLine(cmd).parseWithHandler(new RunLast(), System.err, args);
	}
	
	public File getHomeDir() {
		if ( m_homeDir != null ) {
			return m_homeDir;
		}
		else {
			return FOption.ofNullable(System.getenv(ENVVAR_HOME))
							.map(File::new)
							.getOrElse(Utilities.getCurrentWorkingDir());
		}
	}
	
	@Override
	public void run() {
		try {
			if ( m_verbose ) {
				System.out.println("use home.dir=" + getHomeDir());
			}
			configureLog4j();
			
			JarveySession jarvey = getInitialContext();
			run(jarvey);
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}
	
	public JarveySession getInitialContext() throws Exception {
		if ( m_jarvey == null ) {
			JarveySession.Builder builder = JarveySession.builder()
														.appName("jarvey_application");
			if ( m_dbRootPath != null ) {
//				builder = builder.warehouseRoot(m_dbRootPath);
//				builder = builder.dbRoot(m_dbRootPath);
			}
			m_jarvey = builder.getOrCreate();
		}
		
		return m_jarvey;
	}

	@Override
	public void configureLog4j() throws IOException {
		File propsFile = new File(getHomeDir(), "log4j.properties");
		if ( m_verbose ) {
			System.out.printf("use log4j.properties: file=%s%n", propsFile);
		}
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		
		Map<String,String> bindings = Maps.newHashMap();
		bindings.put("jarvey.home", propsFile.getParentFile().toString());

		String rfFile = props.getProperty("log4j.appender.rfout.File");
		rfFile = StringSubstitutor.replace(rfFile, bindings);
		props.setProperty("log4j.appender.rfout.File", rfFile);
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
	}
}