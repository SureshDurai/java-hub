package com.javahub.keepalive;


import org.apache.log4j.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ClientStatusListenerExt.AutoConnectionStatus;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@SuppressWarnings({ "unused", "rawtypes" })
public class Factory {


	private static Properties properties = new Properties();
	private static int fetchLimit = 0;
	private static String recordSeparator = ",";
	private static String ipaddr = "";
	private static int port = 0;
	private static String voltusername = "";
	private static String voltpassword = "";
	public static Client client = null;
	public static Client insertClient = null;
	private static ClientConfig config = null;
	private static Future futureThread;
	private static Future futureInsertThread;
	private static int backPressureSlpTime;

	static {
		try {
			loadPropertiesAndConfig();
			backPressureSlpTime = PropertiesLoader.getIntValue(PropertiesLoader.BACK_PRESSURE_SLP_TIME);
			initializeVoltdbClient();
			initializeVoltdbInsertClient();

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	static class StatusListener extends ClientStatusListenerExt {
		@Override
		public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
			LOG.error("Connections Left => " + connectionsLeft + " :: Stopping the client, disconnecting from => "
					+ hostname + " :: cause => " + cause);
		}

		@Override
		public void backpressure(boolean status) {
			LOG.error(
					"Too much backpressure..! Status => " + status + " | Sleeping for " + backPressureSlpTime + " ms.");
			try {
				Thread.sleep(backPressureSlpTime);
				LOG.error("Finished sleeping. Resume again.");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void uncaughtException(ProcedureCallback callback, ClientResponse r, Throwable e) {
			LOG.error("uncaughtException Error :: " + callback);
		}

		@Override
		public void lateProcedureResponse(ClientResponse response, String hostname, int port) {
			LOG.error("LateProcedureResponse :: Status => " + response.getStatus());
		}

		@Override
		public void connectionCreated(String hostname, int port, AutoConnectionStatus status) {
			LOG.info("ConnectionCreated to volt node => " + hostname + " :: AutoConnectionStatus => " + status);
		}
	}

	public static Client getInsertClient() throws Exception {
		if (insertClient.getInstanceId() != null) {
			return insertClient;
		} else {
			loadPropertiesAndConfig();
			initializeVoltdbInsertClient();
			return insertClient;
		}

	}

	public static Client getClient() throws Exception {
		if (client.getInstanceId() != null) {
			return client;
		} else {
			loadPropertiesAndConfig();
			initializeVoltdbClient();
			return client;
		}

	}

	private static void loadPropertiesAndConfig() throws Exception {
		loadProperties(DB_PROPERTIES_FILENAME);
		config = new ClientConfig(voltusername, voltpassword, new StatusListener());
		config.setReconnectOnConnectionLoss(
				Boolean.parseBoolean(properties.getProperty("enba.volt.reconnectOnConnectionLoss")));
		config.setClientAffinity(Boolean.parseBoolean(properties.getProperty("enba.volt.clientAffinity")));
		config.setInitialConnectionRetryInterval(
				Integer.parseInt(properties.getProperty("enba.volt.InitialConnectionRetryInterval")));
		config.setMaxConnectionRetryInterval(
				Integer.parseInt(properties.getProperty("enba.volt.maxConnectionRetryInterval")));
		config.setHeavyweight(Boolean.parseBoolean(properties.getProperty("enba.volt.heavyweight")));
		config.setTopologyChangeAware(true);
	}

	private static void initializeVoltdbClient() throws UnknownHostException, IOException {
		client = ClientFactory.createClient(config);
		String[] servers = ipaddr.split(",");
		/*
		 * Instantiate a client and connect to all servers
		 */

		for (String server : servers) {
			try {
				LOG.info("Connecting to volt node = " + server);
				client.createConnection(server, port);
			} catch (ConnectException e) {
				LOG.info("Unable to connect to volt node =  " + server + " :: Error = " + e.getMessage());
			}
			if (null != client && null != client.getInstanceId()) {
				break;
			}
		}

		keepConnectionAlive();
	}

	private static void initializeVoltdbInsertClient() throws UnknownHostException, IOException {
		insertClient = ClientFactory.createClient(config);

		String[] servers = ipaddr.split(",");
		/*
		 * Instantiate a client and connect to all servers
		 */

		for (String server : servers) {
			try {
				LOG.info("Connecting to volt node = " + server);
				insertClient.createConnection(server, port);
			} catch (ConnectException e) {
				LOG.info("Unable to connect to volt node =  " + server + " :: Error = " + e.getMessage());
			}
			if (null != insertClient && null != insertClient.getInstanceId()) {
				break;
			}
		}

		keepInsertConnectionAlive();
	}

	private static void loadProperties(String fileName) throws IOException {
		FileInputStream propsFile = new FileInputStream(fileName);
		properties.load(propsFile);
		propsFile.close();
		fetchLimit = getIntProperty("enba.fetch.limit", 10);
		ipaddr = properties.getProperty("enba.volt.ip.address");
		port = getIntProperty("enba.volt.port", 21212);
		voltusername = properties.getProperty("enba.volt.username");
		voltpassword = Crypt.decrypt(properties.getProperty("enba.volt.key"),
				properties.getProperty("enba.volt.password"));
	}

	private static int getIntProperty(String propName, int defaultValue) {
		return Integer.parseInt(properties.getProperty(propName, Integer.toString(defaultValue)));
	}

	public static void close() throws Exception {
		try {
			if (client != null) {
				client.drain();
				client.close();
			}
			if (insertClient != null) {
				insertClient.drain();
				insertClient.close();
			}

			if (futureThread != null) {
				// Stopping keeper alive thread.
				futureThread.cancel(true);
			}
			if (futureInsertThread != null) {
				// Stopping keeper alive thread.
				futureInsertThread.cancel(true);
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private static void keepConnectionAlive() {
		ExecutorService es = Executors.newFixedThreadPool(1);
		futureThread = es.submit(new KeepAliveThread(client));
		es.shutdown();
	}

	private static void keepInsertConnectionAlive() {
		ExecutorService es = Executors.newFixedThreadPool(1);
		futureInsertThread = es.submit(new KeepAliveThread(insertClient));
		es.shutdown();
	}

}

class KeepAliveThread implements Runnable {
	private Client client;

	public KeepAliveThread(Client client) {
		this.client = client;
	}

	public void run() {
		try {
			synchronized (this) {
				while (true) {
					client.callProcedure("VOLTDB_KEEP_ALIVE_SP","X");
					wait(20000);
				}
			}
		} catch (InterruptedException ex) {
			System.out.println(" Stopping Keeper Alive Thread- ENBAEventManager.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
