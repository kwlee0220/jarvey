package jarvey.command;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.PicocliCommand;
import utils.UsageHelp;
import utils.func.FOption;
import utils.io.FileUtils;

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
	
	private @Nullable File m_homeDir = null;
	
	@Option(names={"-v"}, description={"verbose"})
	protected boolean m_verbose = false;
	
	@Option(names={"--config"}, paramLabel="path", description={"HDFS configuration file path"})
	protected String m_configPath = null;
	
	@Option(names={"--dataset_root"}, paramLabel="path", description={"HDFS dataset root path"})
	protected String m_dbRootPath = null;
	
	private @Nullable JarveySession m_jarvey;
	
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
							.getOrElse(FileUtils.getCurrentWorkingDirectory());
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
			if ( m_configPath != null ) {
				builder = builder.hadoopDatasetRoot(new Path(m_configPath), m_dbRootPath);
			}
			else if ( m_dbRootPath != null ) {
				builder = builder.root(new File(m_dbRootPath));
			}
			else {
				builder = builder.hadoopDatasetRoot(new Path("jarvey-hadoop.xml"), "datasets");
			}
			m_jarvey = builder.getOrCreate();
		}
		
		return m_jarvey;
	}

	@Override
	public void configureLog4j() throws IOException {
		File confFile = new File(getHomeDir(), "log4j2.xml");
		if ( m_verbose ) {
			s_logger.debug("use log4j configuration from {}", confFile);
		}

		Configurator.initialize(null, confFile.getAbsolutePath());
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j configuration from {}", confFile);
		}
	}
}