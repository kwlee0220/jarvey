package jarvey.command;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.JarveySession;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;
import picocli.CommandLine.Spec;
import utils.PicocliCommand;
import utils.UsageHelp;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class JarveyLocalCommand implements PicocliCommand<JarveySession> {
	private static final Logger s_logger = LoggerFactory.getLogger(JarveyLocalCommand.class);
	private static final String ENVVAR_HOME = "JARVEY_HOME";
	
	@Spec protected CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Nullable private File m_homeDir = null;
	
	@Option(names={"-v"}, description={"verbose"})
	protected boolean m_verbose = false;
	
	@Option(names={"--config", "-c"}, paramLabel="path", description={"HDFS configuration file path"})
	protected String m_configPath = null;
	
	@Option(names={"--dataset_root", "-d"}, paramLabel="path", description={"HDFS dataset root path"})
	protected String m_dbRootPath = null;
	
	@Option(names={"--master", "-m"}, paramLabel="url", description={"master-url"})
	protected String m_master = null;
	
	@Option(names={"--num-executors"}, paramLabel="count", description={"Executor count"})
	protected int m_executorCount = -1;
	
	@Nullable private JarveySession m_jarvey;
	
	protected abstract void run(JarveySession jarvey) throws Exception;

	@SuppressWarnings("deprecation")
	public static final void run(JarveyLocalCommand cmd, String... args) throws Exception {
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
			if ( m_master != null ) {
				builder = builder.master(m_master);
			}
			if ( m_executorCount > 0 ) {
				builder = builder.executorCount(m_executorCount);
			}
			if ( m_configPath != null ) {
				builder = builder.hadoopDatasetRoot(new Path(m_configPath), m_dbRootPath);
			}
			else if ( m_dbRootPath != null ) {
				builder = builder.root(new File(m_dbRootPath));
			}
			else {
				builder = builder.hadoopDatasetRoot(new Path("jarvey-hadoop.xml"), "jarvey");
			}
			m_jarvey = builder.getOrCreate();
		}
		
		return m_jarvey;
	}

	@Override
	public void configureLog4j() throws IOException {
		File confFile = new File(getHomeDir(), "log4j2.xml");
		if ( m_verbose ) {
			System.out.printf("use log4j configuration from %s\n", confFile.getAbsolutePath());
		}
		
		File propFile = new File(getHomeDir(), "log4j.properties");
		PropertyConfigurator.configure(propFile.getAbsolutePath());
		if ( m_verbose ) {
			System.out.printf("use log4j1 configuration (just for Spark) from %s\n", propFile.getAbsolutePath());
		}
		
		Configurator.initialize(null, confFile.getAbsolutePath());
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j2 configuration from {}", confFile);
		}
	}
	
	public static void configureLog4j(File homeDir, boolean verbose) throws IOException {
		File confFile = new File(homeDir, "log4j2.xml");
		if ( verbose ) {
			System.out.printf("use log4j configuration from %s\n", confFile.getAbsolutePath());
		}
		
		File propFile = new File(homeDir, "log4j.properties");
		PropertyConfigurator.configure(propFile.getAbsolutePath());
		if ( verbose ) {
			System.out.printf("use log4j1 configuration (just for Spark) from %s\n", propFile.getAbsolutePath());
		}
		
		Configurator.initialize(null, confFile.getAbsolutePath());
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j2 configuration from {}", confFile);
		}
	}
}