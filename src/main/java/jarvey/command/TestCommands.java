package jarvey.command;

import jarvey.JarveySession;
import jarvey.sample.perf.BuildSafeZoneSpeed;
import jarvey.sample.perf.BuildTenMinutePolicy;
import jarvey.sample.perf.CountInvalidGeom;
import jarvey.sample.perf.FindBestSubway;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.PicocliSubCommand;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class TestCommands {
	@Command(name="invalid_geoms", description="count invalid geometry records")
	static class CountInvalidGeomCommand extends PicocliSubCommand<JarveySession> {
		private static final String DEFAULT_REGION = "구역/시군구_2019";

		@Parameters(paramLabel="id", index="0", arity="1..1", description={"target dataset"})
		private String m_input;

		@Option(names= {"-b", "--bounds_dataset"}, paramLabel="count", defaultValue=DEFAULT_REGION,
				description="region dataset (default: '구역/시군구_2019')")
		private String m_boundDsId;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			CountInvalidGeom job = new CountInvalidGeom(jarvey, m_input, m_boundDsId);
			long invalidCount = job.call();
			System.out.printf("done: invalid count=%d, elapsed=%s%n", invalidCount, watch.getElapsedMillisString());

			System.in.read();
			jarvey.spark().stop();
		}
	}
	
	@Command(name="safezone_speed", description="build average vehicle speed near safezones")
	static class BuildSafeZoneSpeedCommand extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="id", index="0", arity="1..1", description={"target dataset"})
		private String m_input;

		@Parameters(paramLabel="ds_id", index="1", arity="1..1", description={"output dataset"})
		private String m_output;

		@Option(names= {"-d", "--distance"}, paramLabel="meter", defaultValue="300.0",
				description="distance meter (default: '300')")
		private double m_dist;

		@Option(names= {"-s", "--min_samples"}, paramLabel="count", defaultValue="1000",
				description="minimum samples (default: '1000')")
		private int m_sampleCount;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			BuildSafeZoneSpeed job = new BuildSafeZoneSpeed(jarvey, m_input, m_output, m_dist, m_sampleCount);
			job.call();
			System.out.printf("done: output=%s, elapsed=%s%n", m_output, watch.getElapsedMillisString());

			System.in.read();
			jarvey.spark().stop();
		}
	}
	
	@Command(name="best_subway_station_spots", description="find best spots for subway stations")
	static class FindBestSubwayStationsCommand extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="output", index="0", arity="1..1", description={"output dataset"})
		private String m_output;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			FindBestSubway job = new FindBestSubway(jarvey, m_output);
			job.call();
			System.out.printf("done: output=%s, elapsed=%s%n", m_output, watch.getElapsedMillisString());

			System.in.read();
			jarvey.spark().stop();
		}
	}
	
	@Command(name="10min_policy", description="build 10-minute policy map")
	static class BuildTenMinutePolicyCommand extends PicocliSubCommand<JarveySession> {
		@Parameters(paramLabel="output", index="0", arity="1..1", description={"output dataset"})
		private String m_output;

		@Override
		public void run(JarveySession jarvey) throws Exception {
			StopWatch watch = StopWatch.start();
			
			BuildTenMinutePolicy job = new BuildTenMinutePolicy(jarvey, m_output);
			job.call();
			System.out.printf("done: output=%s, elapsed=%s%n", m_output, watch.getElapsedMillisString());

			System.in.read();
			jarvey.spark().stop();
		}
	}
}
