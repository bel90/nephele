package eu.stratosphere.nephele.api;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.rpc.ManagementTypeUtils;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.nephele.util.StringUtils;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author bel
 *
 */
public class FaultToleranceAPI {
	
	//private Text pseodoConsoleTextField;
	
	private static final Log LOG = LogFactory.getLog(JobManager.class);
	
	/**
	 * The directory containing the Nephele configuration for this integration test.
	 */
	private static final String CONFIGURATION_DIRECTORY = "correct-conf";
	
	/**
	 * The configuration for the job client;
	 */
	private static Configuration CONFIGURATION;
	
	/**
	 * The directory containing the Nephele configuration for this integration test.
	 */
	private static String CONF_DIR;
	
	/**
	 * If there is a running JobClient, this jobClient is not null
	 */
	private JobClient jobClient = null;
	
	/**
	 * The running jobManager if it exist
	 */
	private JobManager jobManager = null;
	
	/**
	 * Constructor, which is used, when the Configuration is already initialized
	 * @param conf The Configuration Object
	 * @param confDir The directory of the Configuration
	 */
	public FaultToleranceAPI(Configuration conf, String confDir) {
		CONFIGURATION = conf; 
		CONF_DIR = confDir;
	}
	
	/**
	 * Default Constructor. Searches for the Configuration.
	 */
	public FaultToleranceAPI() {

		// Try to find the correct configuration directory
		try {
			final String userDir = System.getProperty("user.dir");
			//LOG.warn(userDir);
			CONF_DIR = userDir + File.separator + CONFIGURATION_DIRECTORY;
			//LOG.warn(CONF_DIR);
			if (!new File(CONF_DIR).exists()) {
				CONF_DIR = userDir + "/src/test/resources/" + CONFIGURATION_DIRECTORY;
				//LOG.warn(CONF_DIR);
			}
		} catch (SecurityException e) {
			fail(e.getMessage());
		} catch (IllegalArgumentException e) {
			fail(e.getMessage());
		} 
		
		CONFIGURATION = GlobalConfiguration.getConfiguration(new String[] { ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY });
		
	}
	
	/**
	 * Connects Nephele with the in the Configuration given IP and Port.
	 * Is used by the function startNephele()
	 * @return The Extendes Management Protocol for further use.
	 * TODO Which further use?
	 */
	public ExtendedManagementProtocol connect() {
		// Try to load global configuration
		GlobalConfiguration.loadConfiguration(CONF_DIR);

		final String address = GlobalConfiguration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		if (address == null) {
			LOG.error("Cannot find address to job manager's RPC service in configuration");
			System.exit(1);
			return null;
		}

		final int port = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);

		if (port < 0) {
			LOG.error("Cannot find port to job manager's RPC service in configuration");
			System.exit(1);
			return null;
		}

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(ManagementTypeUtils.getRPCTypesToRegister());
		} catch (IOException ioe) {
			LOG.error("Error initializing the RPC service: " + StringUtils.stringifyException(ioe));
			System.exit(1);
			return null;
		}

		final InetSocketAddress inetaddr = new InetSocketAddress(address, port);
		ExtendedManagementProtocol jobManager = null;
		int queryInterval = -1;
		try {
			jobManager = rpcService.getProxy(inetaddr, ExtendedManagementProtocol.class);

			// Get the query interval
			queryInterval = jobManager.getRecommendedPollingInterval();
			System.out.println("Query Interval: " + queryInterval);

		} catch (Exception e) {
			e.printStackTrace();
			rpcService.shutDown();
			LOG.warn("Cant connect to RPC Service");
		}
		
		return jobManager;
	}

	/**
	 * Returns the EventList from the current running Job Client, which contains
	 * all occurred Events, since the Job is running.
	 * @return The AbstractEvent ArrayList of the current running JobClient
	 */
	public ArrayList<AbstractEvent> getJobClientEventList() {
		if (this.jobClient != null) {
			return this.jobClient.getEventList();
		}
		return null;
	}
	
}

