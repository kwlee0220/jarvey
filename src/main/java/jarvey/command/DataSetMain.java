package jarvey.command;

import jarvey.JarveySession;
import picocli.CommandLine.Command;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="dataset-related commands",
		subcommands = {
			DatasetCommands.ListDataSet.class,
			DatasetCommands.Show.class,
			DatasetCommands.Schema.class,
			DatasetCommands.Move.class,
			DatasetCommands.Import.class,
			DatasetCommands.Export.class,
			DatasetCommands.Delete.class,
			DatasetCommands.Cluster.class,
		})
public class DataSetMain extends JarveyLocalCommand {
	public static final void main(String... args) throws Exception {
		run(new DataSetMain(), args);
	}

	@Override
	protected void run(JarveySession jarvey) throws Exception { }
}
