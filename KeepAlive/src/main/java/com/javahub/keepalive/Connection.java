package com.javahub.keepalive;

import static com.javahub.ussd.utils.USSDConstants.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import ie.omk.smpp.Address;
import ie.omk.smpp.AlreadyBoundException;
import ie.omk.smpp.BadCommandIDException;
import ie.omk.smpp.Connection;
import ie.omk.smpp.event.ReceiverExitEvent;
import ie.omk.smpp.event.SMPPEventAdapter;
import ie.omk.smpp.message.BindResp;
import ie.omk.smpp.message.DeliverSM;
import ie.omk.smpp.message.DeliverSMResp;
import ie.omk.smpp.message.EnquireLink;
import ie.omk.smpp.message.EnquireLinkResp;
import ie.omk.smpp.message.InvalidParameterValueException;
import ie.omk.smpp.message.SMPPPacket;
import ie.omk.smpp.message.SMPPProtocolException;
import ie.omk.smpp.message.SMPPRequest;
import ie.omk.smpp.message.SMPPResponse;
import ie.omk.smpp.message.SubmitSM;
import ie.omk.smpp.message.SubmitSMResp;
import ie.omk.smpp.message.Unbind;
import ie.omk.smpp.message.UnbindResp;
import ie.omk.smpp.message.tlv.Tag;
import ie.omk.smpp.util.AlphabetEncoding;
import ie.omk.smpp.util.EncodingFactory;
import ie.omk.smpp.util.MessageEncoding;
import ie.omk.smpp.util.SequenceNumberScheme;
import ie.omk.smpp.version.VersionException;

public class Connection {

	private static final Logger LOG = Logger.getLogger(UssdConnectionFactory.class);

	private static Properties properties = new Properties();
	private final static int ESME_ROK = 0x00000000;
	private final static int ESME_RTHROTTLED = 0x00000058;
	private final static int ESME_RMSGQFUL = 0x00000014;
	private static boolean isClearActiveSession = true;
	private final static int ACTIVE = 2;
	private final AtomicLong lastActivityTime = new AtomicLong(System.currentTimeMillis());
	private static String ussdHostName = "";
	private static int ussdPort = 0;
	private static String ussdSystemId = "";
	private static String ussdSystemPassword = "";
	private static String ussdSystemType = "";
	private static int esmeTON = 0;
	private static int esmeNPI = 1;
	private static String esmeAddressRangeRegex = "";
	private static int keepAliveCycleInSeconds = 0;
	private static int bufferSize = 0;
	private static int maxMessagesPerSecond = 0;
	private static int index = 0;
	private static int retryCount = 0;
	private static int retryPollCycleSeconds = 60;
	private int retryPollCycleMilliSec = 0;
	private static boolean displayFlag;

	private int connectionStatus;
	public boolean connectionReset = false;
	private static LinkedBlockingDeque<TransmitMessage> transmitMessageBuffer;
	private final LinkedHashMap<Integer, TransmitMessage> transmitMessageCache;
	private static Map<Integer, UssdSession> activeSessions = new ConcurrentHashMap<Integer, UssdSession>();

	private final SequenceNumberScheme sequenceNumberScheme;
	private final TransmitDelay transmitDelay;
	private Connection ussdConnection;
	private ReentrantLock reentrantLock;
	private Condition isConnected;
	private MessageListenerImpl messageListenerImpl;
	private final MessageTransmitter transmitter;

	private ScheduledExecutorService keepAliveExecutorService;
	private ScheduledExecutorService messageTransmitterExecutorService;
	private ScheduledExecutorService removeInactiveSessionsService;
	private ScheduledExecutorService retryExcService;

	private static List<String> ussdIngReqList;
	private static Map<String, String> mlRefreshFlagMap;
	private static Map<String, String> mlSubMenuProdTypeMenu;
	private static LookUpDAO lookUpDAO;
	private static UpdaterDAO updaterDao;
	private Map<Integer, UserInfo> menuLevelOne = new ConcurrentHashMap<>();
	private Map<Integer, UserInfo> menuLevelTwo = new ConcurrentHashMap<>();

	private Map<Integer, UserInfo> ragMainMenu = new ConcurrentHashMap<>();
	private Map<Integer, UserInfo> ragSubMenu = new ConcurrentHashMap<>();

	static {
		try {
			loadProperties(USSD_CONNECTION_PROPERTIES_FILE);
			ussdIngReqList = Arrays.asList(PropertiesLoader.getValue(USSD_INCOMING_REQ_CODE).split(","));
			displayFlag = PropertiesLoader.getValue(USSD_ACCOUNT_DISP_FLAG).equalsIgnoreCase("true") ? true : false;
			mlRefreshFlagMap = loadMlRefreshFlagMap();
			mlSubMenuProdTypeMenu = getProdTypeMap();
		} catch (Exception e) {
			LOG.error("Error in loading the USSD Connection Properties File", e);
		}
	}

	public Connection() throws Exception {
		reentrantLock = new ReentrantLock();
		isConnected = reentrantLock.newCondition();
		transmitMessageBuffer = new LinkedBlockingDeque<>(bufferSize);
		sequenceNumberScheme = new UssdSequenceNumberScheme(index);
		transmitMessageCache = new LinkedHashMap<Integer, TransmitMessage>(bufferSize) {
			private static final long serialVersionUID = 1L;

			@Override
			protected boolean removeEldestEntry(Map.Entry<Integer, TransmitMessage> eldest) {
				return size() > bufferSize;
			}
		};
		transmitDelay = new TransmitDelay(maxMessagesPerSecond);
		transmitter = new MessageTransmitter();
		retryPollCycleMilliSec = retryPollCycleSeconds * 1000;
		lookUpDAO = new LookUpDAO();
		updaterDao = new UpdaterDAO();
	}

	private static Map<String, String> loadMlRefreshFlagMap() throws Exception {
		mlRefreshFlagMap = new HashMap<String, String>();
		List<String> offerRefreshFlagList = Arrays
				.asList(PropertiesLoader.getValue(USSD_OFFER_REFRESH_FLAG_MAP).split(","));
		for (String key : offerRefreshFlagList) {
			String mapAry[] = key.split(":");
			mlRefreshFlagMap.put(mapAry[0], mapAry[1]);
		}

		return mlRefreshFlagMap;
	}

	private static Map<String, String> getProdTypeMap() {
		List<String> prodTypeList;
		try {
			mlSubMenuProdTypeMenu = new HashMap<String, String>();
			prodTypeList = Arrays.asList(PropertiesLoader.getValue(USSD_SUB_MENU_PROD_TYPE).split(","));
			for (String key : prodTypeList) {
				String prodMapping[] = key.split(":");
				mlSubMenuProdTypeMenu.put(prodMapping[0], prodMapping[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return mlSubMenuProdTypeMenu;
	}

	/*
	 * Load Properties File and set the values
	 */
	private static void loadProperties(String fileName) throws IOException {
		FileInputStream fileInputStream = new FileInputStream(fileName);
		properties.load(fileInputStream);
		ussdHostName = properties.getProperty("ussd.hostname");
		ussdPort = Integer.parseInt(properties.getProperty("ussd.port"));
		ussdSystemId = properties.getProperty("ussd.systemId");
		ussdSystemPassword = properties.getProperty("ussd.systemPassword");
		ussdSystemType = properties.getProperty("ussd.systemType");
		esmeTON = Integer.parseInt(properties.getProperty("ussd.esmeTON"));
		esmeNPI = Integer.parseInt(properties.getProperty("ussd.esmeNPI"));
		esmeAddressRangeRegex = properties.getProperty("ussd.esmeAddressRangeRegex");
		keepAliveCycleInSeconds = Integer.parseInt(properties.getProperty("ussd.keepAliveCycleInSeconds"));
		bufferSize = Integer.parseInt(properties.getProperty("ussd.queueBufferSize"));
		retryPollCycleSeconds = Integer.parseInt(properties.getProperty("ussd.retryPollCycleSeconds"));
		retryCount = Integer.parseInt(properties.getProperty("ussd.num.retries"));
	}

	/*
	 * Bind the Connection to USSD Gateway
	 */
	public void bind() throws UssdConnectionException, InterruptedException {
		String fullIdentifier = "(" + esmeAddressRangeRegex + ")" + ussdSystemId + "@" + ussdHostName + ":" + ussdPort;
		try {
			ussdConnection = new Connection(ussdHostName, ussdPort, IS_ASYNC);
		} catch (UnknownHostException e) {
			throw new UssdConnectionException("Unable to connect to the USSD Gateway as " + fullIdentifier);
		}

		String logDetail = displayFlag ? "Connected to USSD Gateway as " + fullIdentifier : "Connected to USSD Gateway";
		LOG.info(logDetail);
		ussdConnection.addObserver(new ReceiverObserver());

		LOG.info("Binding to USSD for sending messages");
		@SuppressWarnings("unused")
		boolean isTimedOut = false;
		reentrantLock.lock();
		try {
			LOG.info("Binding to USSD GW for Transmitting Messages");
			ussdConnection.bind(Connection.TRANSCEIVER, ussdSystemId, ussdSystemPassword, ussdSystemType, esmeTON,
					esmeNPI, esmeAddressRangeRegex);
			LOG.info("Bound to USSD GW for Transmitting Messages");
			isTimedOut = !(isConnected.await(CONNECTION_TIMEOUT, TimeUnit.SECONDS));
			this.setConnectionStatus(ussdConnection.getState());
			this.setMessageListener(new MessageListenerImpl());
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new UssdConnectionException(
					"Unable to bind the connection to USSD GW as " + fullIdentifier + " with error " + e.getMessage(),
					e);
		} finally {
			reentrantLock.unlock();
		}

		keepAliveExecutorService = Executors.newSingleThreadScheduledExecutor();
		messageTransmitterExecutorService = Executors.newSingleThreadScheduledExecutor();
		removeInactiveSessionsService = Executors.newSingleThreadScheduledExecutor();

		// Thread to transmit the messages every 1millisecond
		messageTransmitterExecutorService.scheduleWithFixedDelay(transmitter, 1, 1, TimeUnit.MILLISECONDS);
		removeInactiveSessionsService.submit(new TimeoutHandler());
		isClearActiveSession = true;
		// removeInactiveSessionsService.scheduleWithFixedDelay(new
		// TimeoutHandler(), 10, 10, TimeUnit.MILLISECONDS);

		// Keep Alive Worker Thread
		if (keepAliveCycleInSeconds != 0) {
			keepAliveExecutorService.scheduleWithFixedDelay(new KeepConnectionAlive(ussdConnection),
					keepAliveCycleInSeconds, keepAliveCycleInSeconds, TimeUnit.SECONDS);
		}
		if (null == retryExcService || retryExcService.isShutdown()) {
			retryExcService = Executors.newScheduledThreadPool(5);
			retryExcService.scheduleWithFixedDelay(new RetryConnection(), retryPollCycleSeconds, retryPollCycleSeconds,
					TimeUnit.SECONDS);
		}

		LOG.info("USSD Binding Completed!!!!! ");

		if (connectionReset) {
			connectionReset = false;
			LOG.debug("Setting connectionReset => " + connectionReset);
		}
	}

	/*
	 * Unbind the connection to USSD gateway
	 */
	public void unbind() throws UssdConnectionException {
		if (ussdConnection == null) {
			return;
		}
		keepAliveExecutorService.shutdown();
		messageTransmitterExecutorService.shutdown();
		removeInactiveSessionsService.shutdown();
		isClearActiveSession = false;
		activeSessions.clear();

		try {
			keepAliveExecutorService.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOG.error("Exception in making the Keep Alive Thread to wait", e);
		}

		if (ussdConnection.isBound()) {
			LOG.info("Sending Unbind request to USSD Gateway");
			boolean isTimedOut = false;
			reentrantLock.lock();
			try {
				ussdConnection.unbind();
				LOG.info("Connectiong to USSD Gateway is unbound");
				isTimedOut = !(isConnected.await(CONNECTION_TIMEOUT, TimeUnit.SECONDS));
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("Error unbinding the connection to USSD Gateway", e);
			} finally {
				reentrantLock.unlock();
			}
			LOG.info("Closing the Network Link to USSD Gateway");
			try {
				ussdConnection.closeLink();
			} catch (Exception e) {
				e.printStackTrace();
				throw new UssdConnectionException("Unable to close the network link to USSD Gateway", e);
			} finally {
				ussdConnection = null;
			}

			LOG.info("Closed the network link to USSD Gateway");

			if (isTimedOut) {
				throw new UssdConnectionException("Unable to receive the Unbind Response");
			}
		}
	}

	/*
	 * Method used to set the message parameters
	 */
	public void sendMessage(UssdMessage ussdMessage) {
		LOG.debug("Into Send Message Method");
		TransmitMessage message = new TransmitMessage();
		message.setSourceAddress(ussdMessage.getSourceAddress());
		message.setDestinationAddress(ussdMessage.getDestinationAddress());
		message.setMessageText(ussdMessage.getMessageText());
		message.setReferenceNumber(ussdMessage.getSessionId());
		message.setServiceOp(ussdMessage.getServiceOp());
		addToSendList(message);
	}

	public void sendMessage(UserInfo userInfo) throws Exception {
		try {
			LOG.debug("Into UserInfo Send Message Method");
			TransmitMessage message = new TransmitMessage();
			message.setSourceAddress(userInfo.getDestAddress());
			message.setDestinationAddress(userInfo.getMsisdn());
			message.setMessageText(userInfo.getMessageBody());
			message.setTransactionId(userInfo.getTxId());
			message.setServiceOp(2);
			message.setReferenceNumber(userInfo.getUserMsgRef());
			message.setUssdLogStatus(2); // setting 2 for ussd log table status
			message.setProdIds(userInfo.getProdIds());
			message.setMlFlag(userInfo.getMlFlag());

			addToSendList(message);

			if (userInfo.isUpdateCCR()) {
				LOG.debug("Update Reduced CCR || MSISDN => " + userInfo.getMsisdn() + " | Prod Type => "
						+ mlRefreshFlagMap.get(userInfo.getSelProdType()) + " | Offer Refresh Flag => "
						+ userInfo.getOfferRefreshFlag());
				updateReducedCCR(userInfo.getMsisdn(), mlRefreshFlagMap.get(userInfo.getSelProdType()),
						userInfo.getOfferRefreshFlag());
			}
		} catch (Exception e) {
			LOG.error("Exception inside sendMessage: " + e);
		}

	}

	private void updateReducedCCR(String subscriberId, String prodType, String offerRefFlag) throws Exception {
		// String offerRefFalg = lookUpDAO.get
		// //rewardsDAO.getOfferRefreshFlag(subscriberId);
		if (offerRefFlag.indexOf(prodType) >= 0) {
			/*
			 * if reward provision is not success, need to remove the data/voice/integrated
			 * from OFFER_REFRESH_FLAG column
			 */
			if (offerRefFlag.equalsIgnoreCase(prodType)) {
				offerRefFlag = VALUE_N;
			} else {
				offerRefFlag = offerRefFlag.replace(prodType, "");
			}
			LOG.debug("Updating ERED_T_REDUCED_CCR [OFFER_REFRESH_FLAG] = " + offerRefFlag + " for MSISDN = "
					+ subscriberId);
			updaterDao.updateReducedCCR(offerRefFlag, subscriberId);
		}
	}

	/*
	 * Method to add all the list of messages that needs to be sent into a Linked
	 * Blocking Queue
	 */
	private void addToSendList(TransmitMessage message) {
		try {
			transmitMessageBuffer.add(message);
		} catch (Exception e) {
			LOG.error("Exception inside addToSendList: " + e);
		}
	}

	private boolean verifyToRestart() {
		boolean restart = false;
		if ((EnquiryLinkTime.enquireLinkRequestTime.get() > EnquiryLinkTime.enquireLinkResponseRecievedTime.get())
				&& ((EnquiryLinkTime.enquireLinkRequestTime.get()
						- EnquiryLinkTime.enquireLinkResponseRecievedTime.get()) > retryPollCycleMilliSec)) {
			restart = true;
			LOG.debug("Retry Just For You USSD Process :: RESTART => " + restart);
		} else {
			restart = false;
			LOG.debug("Retry Just For You USSD Process :: RESTART => " + restart);
		}
		return restart;
	}

	/*
	 * Inner Class Message Transmitter which is used to Transmit the messages to the
	 * User
	 */
	private class MessageTransmitter implements Runnable {

		private LinkedList<TransmitMessage> messageSegments = new LinkedList<TransmitMessage>();

		@Override
		public void run() {
			try {
				while (hasMessages(1, TimeUnit.MILLISECONDS)) {
					try {
						sendMessage(messageSegments.peek());
						messageSegments.removeFirst();
					} catch (AlreadyBoundException | VersionException | SMPPProtocolException
							| UnsupportedOperationException | InterruptedException | IOException e) {
						LOG.error("Exception in Message Transmitter to call the sendMessage", e);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("Error = " + e.getMessage());
			}
		}

		/*
		 * Method which verifies whether there are messages to send or not
		 */
		private boolean hasMessages(long timeout, TimeUnit unit) throws Exception {
			if (hasMessagesInBuffer()) {
				return true;
			}
			TransmitMessage message = null;
			try {
				message = transmitMessageBuffer.poll(timeout, unit);
			} catch (InterruptedException e) {
				LOG.error("Exception in polling the messages from the buffer", e);
			}
			if (message == null) {
				return false;
			}

			if (isSplitMessage(message)) {
				messageSegments.add(message);
				return true;
			}

			List<String> segments = MessageUtils.splitMessageText(message.getMessageText(),
					message.getMessageEncoding(), "#BREAK#", 160);
			int size = segments.size();
			if (size == 0) {
				segments.add("");
				size = 1;
			}
			long messageKey = MessageId.nextMessageId(size);
			for (int i = 0; i < size; i++) {
				TransmitMessage messageSegment = new TransmitMessage(message);
				messageSegment.setMessageKey(messageKey + i);
				messageSegment.setMessageText(segments.get(i));
				messageSegment.setPartNumber(i + 1);
				messageSegment.setPartsTotal(size);
				messageSegment.setPartsReference((int) (messageKey & 0xFFFF));
				messageSegment.setTransactionId(message.getTransactionId());
				messageSegments.add(messageSegment);
			}
			return true;
		}

		private boolean hasMessagesInBuffer() {
			return (messageSegments.size() != 0);
		}

		private boolean isSplitMessage(TransmitMessage message) {
			return (message.getMessageKey() != null);
		}

		/*
		 * Method to Transmit Message via USSD Gateway
		 */
		private void sendMessage(TransmitMessage message)
				throws InterruptedException, SocketTimeoutException, AlreadyBoundException, VersionException,
				SMPPProtocolException, UnsupportedOperationException, IOException {
			if (isDestinationAddressInvalid(message) || isTextMessageBlank(message)) {
				return;
			}

			int sequenceNumber = sequenceNumberScheme.nextNumber();
			SubmitSM submitMessage = null;

			try {
				submitMessage = createSubmitMessage(message);
				submitMessage.setSequenceNum(sequenceNumber);
			} catch (final InvalidParameterValueException e) {
				String errorMessage = "Invalid parameter in communication to send to destination: "
						+ message.getDestinationAddress() + " and message key: " + message.getMessageKey();
				LOG.error(errorMessage, e);
				return;
			}

			long interval = transmitDelay.pause(message.getRetryCount() + 1);
			lastActivityTime.set(System.currentTimeMillis());

			LOG.debug("SubmitSM: " + getMessageAsString(submitMessage, message, interval));

			reentrantLock.lock();
			try {
				ussdConnection.sendRequest(submitMessage);
			} finally {
				reentrantLock.unlock();
			}
			messageListenerImpl.logInfo(message.getReferenceNumber(), message);
			transmitMessageCache.put(sequenceNumber, message);
		}

		/*
		 * Method to check whether Destination Address to send message is valid or not
		 */
		private boolean isDestinationAddressInvalid(TransmitMessage message) {
			if (message.getDestinationAddress() != null && message.getDestinationAddress().trim().length() > 0) {
				return false;
			}
			LOG.error("The message has no destination address for Message Key: " + message.getMessageKey());
			LOG.debug(message);

			return true;
		}

		/*
		 * Is Message blank to send
		 */
		private boolean isTextMessageBlank(TransmitMessage message) {
			if (message.getMessageText() != null && message.getMessageText().trim().length() > 0) {
				return false;
			}
			LOG.warn("No message text to send to destination: " + message.getDestinationAddress() + " and message key: "
					+ message.getMessageKey());
			LOG.debug(message);

			return true;
		}

		/*
		 * Create a Submit SM Message
		 */
		private SubmitSM createSubmitMessage(TransmitMessage message) {
			SubmitSM result = null;

			final Address source = new Address(message.getSourceTON(), message.getSourceNPI(),
					message.getSourceAddress() != null ? message.getSourceAddress().trim() : null);

			final Address destination = new Address(message.getDestinationTON(), message.getDestinationNPI(),
					message.getDestinationAddress().trim());

			try {
				result = (SubmitSM) ussdConnection.newInstance(SMPPPacket.SUBMIT_SM);
				if (source.getAddress() != null) {
					result.setSource(source);
				}
				result.setDestination(destination);

				result.setMessageEncoding(EncodingFactory.getInstance().getEncoding(message.getMessageEncoding()));
				result.setOptionalParameter(Tag.USER_MESSAGE_REFERENCE,
						new AtomicInteger((int) (message.getReferenceNumber() & 0xFFFF)));
				result.setOptionalParameter(Tag.USSD_SERVICE_OP, (short) (message.getServiceOp() & 0x7FFF));

				if (message.getPartsTotal() > 1) {
					setMultiPartMessageText(result, message);
				} else {
					result.setMessageText(message.getMessageText());
				}

				result.setDataCoding(message.getDataCoding());
			} catch (final BadCommandIDException e) {
				// This should never end up in here.
				LOG.error("Cannot create SubmitSM SMPP message object", e);
			}

			return result;
		}

		/*
		 * Create Multipart Message as the SMPP API used supports till 157 characters
		 */
		private void setMultiPartMessageText(SubmitSM result, TransmitMessage message) {
			result.setEsmClass(0x40);

			short ref = (short) message.getPartsReference();
			byte[] udh = { (byte) 0x6, (byte) 0x8, (byte) 0x4, (byte) ((ref >> 8) & 0xFF), (byte) (ref & 0xFF),
					(byte) message.getPartsTotal(), (byte) message.getPartNumber() };
			result.setMessage(udh, 0, udh.length, null);

			MessageEncoding encoding = result.getMessageEncoding();
			if (!(encoding instanceof AlphabetEncoding)) {
				encoding = EncodingFactory.getInstance().getDefaultAlphabet();
			}
			AlphabetEncoding a = (AlphabetEncoding) encoding;
			byte[] text = a.encodeString(message.getMessageText());
			byte[] msg = new byte[udh.length + text.length];
			System.arraycopy(udh, 0, msg, 0, udh.length);
			System.arraycopy(text, 0, msg, udh.length, text.length);
			result.setMessage(msg);
		}

		/*
		 * Get the message to be sent as String
		 */
		private String getMessageAsString(final SMPPRequest smppRequest, final TransmitMessage message,
				final long interval) {
			final StringBuffer s = new StringBuffer();
			s.append("message-key=");
			s.append(message == null ? "null" : message.getMessageKey());
			s.append('\t');
			s.append("smpp-header=");
			s.append(smppRequest.getLength());
			s.append(':');
			s.append(smppRequest.getCommandId());
			s.append(':');
			s.append(smppRequest.getCommandStatus());
			s.append(':');
			s.append(smppRequest.getSequenceNum());
			s.append('\t');
			s.append("service-type=");
			s.append(smppRequest.getServiceType());
			s.append('\t');
			s.append("source-addr=");
			if (smppRequest.getSource() == null) {
				s.append("null\t");
			} else {
				s.append(smppRequest.getSource().getTON());
				s.append(':');
				s.append(smppRequest.getSource().getNPI());
				s.append(':');
				s.append(smppRequest.getSource().getAddress());
				s.append('\t');
			}
			s.append("dest-addr=");
			s.append(smppRequest.getDestination().getTON());
			s.append(':');
			s.append(smppRequest.getDestination().getNPI());
			s.append(':');
			s.append(smppRequest.getDestination().getAddress());
			s.append('\t');
			s.append("esm-class=binary:");
			s.append(Integer.toBinaryString(smppRequest.getEsmClass()));
			s.append('\t');
			s.append("protocol-id=");
			s.append(smppRequest.getProtocolID());
			s.append('\t');
			s.append("priority-flag=");
			s.append(smppRequest.getPriority());
			s.append('\t');
			s.append("schedule-delivery-time=");
			s.append(smppRequest.getDeliveryTime());
			s.append('\t');
			s.append("validity-period=");
			s.append(smppRequest.getExpiryTime());
			s.append('\t');
			s.append("registered-delivery=binary:");
			s.append(Integer.toBinaryString(smppRequest.getRegistered()));
			s.append('\t');
			s.append("replace-if-present-flag=");
			s.append(smppRequest.getReplaceIfPresent());
			s.append('\t');
			s.append("data-coding=");
			s.append(smppRequest.getDataCoding());
			s.append('\t');
			s.append("sm-default-msg-id=");
			s.append(smppRequest.getDefaultMsg());
			s.append('\t');
			s.append("user-message-reference=");
			s.append(smppRequest.getOptionalParameter(Tag.USER_MESSAGE_REFERENCE));

			s.append('\t');
			s.append("ussd-service-op=");
			s.append(smppRequest.getOptionalParameter(Tag.USSD_SERVICE_OP));

			s.append('\t');
			s.append("sm-length=");
			s.append(smppRequest.getMessageLen());
			s.append('\t');
			s.append("pause-duration=");
			s.append(interval);
			s.append('\t');
			byte[] msg = smppRequest.getMessage();
			if (msg != null && msg.length > 0) {
				if (message != null && message.getPartsTotal() > 1) {
					final StringBuffer ms = new StringBuffer();
					try {
						int length = (int) msg[0];
						ms.append("udh-length=");
						ms.append(length);
						ms.append('\t');
						ms.append("udh-msg-ref-num=");
						if (msg[0] == (byte) 0x5) {
							ms.append((int) msg[3]);
						} else {
							ms.append((int) ((msg[3] << 8) + (msg[4] & 0xff)));
						}
						ms.append('\t');
						ms.append("udh-total-segments=");
						ms.append((int) msg[length - 1]);
						ms.append('\t');
						ms.append("udh-segment-seqnum=");
						ms.append((int) msg[length]);
						ms.append('\t');
						ms.append("short-message=");
						MessageEncoding encoding = smppRequest.getMessageEncoding();
						if (encoding instanceof AlphabetEncoding) {
							ms.append(((AlphabetEncoding) encoding)
									.decodeString(Arrays.copyOfRange(msg, length + 1, msg.length)));
						}
					} catch (Exception e) {
						ms.append("short-message=");
						ms.append(smppRequest.getMessageText());
					} finally {
						s.append(ms);
					}
				} else {
					s.append("short-message=");
					s.append(smppRequest.getMessageText());
				}
			}
			return s.toString();
		}
	}

	/*
	 * Inner Class - Observer for User Inputs
	 */
	private class ReceiverObserver extends SMPPEventAdapter {

		@Override
		public void submitSMResponse(Connection connection, SubmitSMResp submitSMResp) {
			final long receivedAt = System.currentTimeMillis();

			lastActivityTime.set(receivedAt);

			if (submitSMResp.getCommandStatus() == ESME_RTHROTTLED
					|| submitSMResp.getCommandStatus() == ESME_RMSGQFUL) {
				TransmitMessage message = transmitMessageCache.get(submitSMResp.getSequenceNum());
				if (message != null) {
					LOG.debug("MESSAGE THROLLED Or USSD GATEWAY Q FULL - " + message.serializeToString());
					addToSendList(message);
				}
			} else {
				transmitMessageCache.remove(submitSMResp.getSequenceNum());
			}
		}

		// Handle message delivery. This method does not need to acknowledge the
		// deliver_sm message as we set the Connection object to
		// automatically acknowledge them.
		@Override
		public void deliverSM(Connection connection, DeliverSM deliverSM) {
			final long receivedAt = System.currentTimeMillis();
			lastActivityTime.set(receivedAt);
			int status = deliverSM.getCommandStatus();
			if (status != 0) {
				LOG.error("DeliverSM: !Error! status = " + status);
				return;
			}
			String messageBody = null;

			if (deliverSM.getMessageLen() == 0 || deliverSM.getOptionalParameter(Tag.MESSAGE_PAYLOAD) != null) {
				try {
					messageBody = new String((byte[]) deliverSM.getOptionalParameter(Tag.MESSAGE_PAYLOAD), "UTF-8");
				} catch (UnsupportedEncodingException e) {
					// UTF-8 is used
					LOG.error("Exception in accepting the UTF-8 Encoding", e);
				}
			} else {
				messageBody = deliverSM.getMessageText();
			}

			LOG.debug("DeliverSM: \" messageAsString:" + transmitter.getMessageAsString(deliverSM, null, 0L) + "\"");

			TransmitMessage request = null;

			int serviceOp = deliverSM.getOptionalParameter(Tag.USSD_SERVICE_OP) != null
					? ((Integer) deliverSM.getOptionalParameter(Tag.USSD_SERVICE_OP)).intValue()
					: -1;

			String msisdn = deliverSM.getSource().getAddress();

			LOG.debug("msisdn ==> " + msisdn + " :: messageBody ==> " + messageBody + " :: Serive OP ==>" + serviceOp);
			if (messageBody != null && serviceOp > 0) {
				try {
					Integer sessionId = (Integer) deliverSM.getOptionalParameter(Tag.USER_MESSAGE_REFERENCE);
					LOG.debug("User_Message_Reference :: sessionId = " + sessionId);
					if (sessionId != null) {
						if (ussdIngReqList.contains(messageBody)) {
							UserInfo userInfo = lookUpDAO.getUserInfo(msisdn);
							if (null != userInfo && userInfo.isJFUEligible()) {
								switch (serviceOp) {
								// service op 1 --> *2222# --> PPSR/PSSN
								case 1:
									// ML Flow
									if (userInfo.getMlFlag()) {
										userInfo.setMsisdn(msisdn);
										userInfo.setTxId(new Utils().getTransactionID());
										userInfo.setUserMsgRef(sessionId);
										userInfo.setDestAddress(deliverSM.getDestination().getAddress());

										// put in menuLevelOne
										menuLevelOne.put(sessionId, userInfo);

										// remove from menuLevelTwo
										menuLevelTwo.remove(sessionId);
										ragMainMenu.remove(sessionId);
										ragSubMenu.remove(sessionId);
										// sending menu
										request = initialMLRequest(sessionId, userInfo, messageBody);
									} else {
										// J4U Flow
										UssdMessage ussdMessage = getUSSDMessageModel(deliverSM, messageBody,
												sessionId);
										request = onReceivedPSSR(sessionId, ussdMessage);
										if (request != null) {
											request.setReferenceNumber(sessionId);
										}
									}

									break;

								// service op 18 --> 1,2,3,4
								case 18:
									if (!messageBody.equals(
											PropertiesLoader.getValue(USSD_ML_RAG_MAIN_MENU_RECHARGE_AND_GET_SELECTION))
											&& ragMainMenu.containsKey(sessionId)
											&& ragSubMenu.containsKey(sessionId)) {
										request = ragOfferInfoRequest(sessionId, messageBody, userInfo);
										ragMainMenu.put(sessionId, userInfo);
										ragSubMenu.put(sessionId, userInfo);
									}
									if (!messageBody.equals(
											PropertiesLoader.getValue(USSD_ML_RAG_MAIN_MENU_RECHARGE_AND_GET_SELECTION))
											&& ragMainMenu.containsKey(sessionId)
											&& !(ragSubMenu.containsKey(sessionId))) {
										request = ragSubMenuRequest(sessionId, messageBody, userInfo);
										ragMainMenu.put(sessionId, userInfo);
										ragSubMenu.put(sessionId, userInfo);
									}
									if (messageBody.equals(
											PropertiesLoader.getValue(USSD_ML_RAG_MAIN_MENU_RECHARGE_AND_GET_SELECTION))
											&& !(ragMainMenu.containsKey(sessionId))
											&& !(ragSubMenu.containsKey(sessionId))) {
										request = ragMainMenuRequest(sessionId, messageBody, userInfo);
										ragMainMenu.put(sessionId, userInfo);
										ragSubMenu.remove(sessionId);
									}
									if (menuLevelTwo.containsKey(sessionId)
											&& msisdn.equalsIgnoreCase(menuLevelTwo.get(sessionId).getMsisdn())
											&& !(ragMainMenu.containsKey(sessionId))
											&& !(ragSubMenu.containsKey(sessionId))) {
										// ML flow
										// provison the reward and notify the
										// user
										UserInfo menuLevelTwoInfo = menuLevelTwo.get(sessionId);
										request = provisionRewardAndNotifyUser(sessionId, messageBody,
												menuLevelTwoInfo);

										// Removing from Both the sessions after
										// provisioning the reward
										menuLevelOne.remove(sessionId);
										menuLevelTwo.remove(sessionId);
									}
									if (menuLevelOne.containsKey(sessionId)
											&& msisdn.equalsIgnoreCase(menuLevelOne.get(sessionId).getMsisdn())
											&& !(ragMainMenu.containsKey(sessionId))
											&& !(ragSubMenu.containsKey(sessionId))) {

										// ML flow
										UserInfo menuLevelOneInfo = menuLevelOne.get(sessionId);

										// check random user or target user
										if (menuLevelOneInfo.getRandomFlag()) {

											LOG.info("Random User :: " + menuLevelOneInfo.getMsisdn());
											// get 3 random offers from
											// ECMP_T_PROD_INFO table based on
											// submenu type
											// send true for random user
											request = subMenuMLRequest(sessionId, messageBody, menuLevelOneInfo, true);

											// send to user and put in
											// menuLevelTwo
											menuLevelOneInfo.setMessageBody(messageBody);
											menuLevelTwo.put(sessionId, menuLevelOneInfo);
										} else {
											// target user
											LOG.info("Target User :: " + menuLevelOneInfo.getMsisdn());
											String offerRefFlag = mlRefreshFlagMap.get(messageBody);
											menuLevelOneInfo.setSelProdType(offerRefFlag);

											if (menuLevelOneInfo.getOfferRefreshFlag().equalsIgnoreCase(VALUE_N)
													|| menuLevelOneInfo.getOfferRefreshFlag()
															.indexOf(offerRefFlag) == -1) {
												// No need to refresh the
												// Offers, already refreshed for
												// today
												// send false for random user
												request = subMenuMLRequest(sessionId, messageBody, menuLevelOneInfo,
														false);

												// send to user and put in
												// menuLevelTwo
												menuLevelOneInfo.setMessageBody(messageBody);
												menuLevelTwo.put(sessionId, menuLevelOneInfo);
											} else {

												/*
												 * Offer needs to be refreshed call ocs plugin to get the balance 1.
												 * send a message to ocs plugin 2. read the response & check the status
												 * Success/Failure 3. Success --> get customer balance and based on
												 * balance, offer rank & pick 3 offers 4. If no 3 offers, then use
												 * expected value column desc and take remaining offers 5. If no offers,
												 * send default notify 6. OCS Failure --> balance 0, then follow steps
												 * 3,4,5
												 */

												LOG.info("Asking user balance from OCS for the msisdn = " + msisdn);

												menuLevelOneInfo.setMessageBody(messageBody);
												messageListenerImpl.logInfo(sessionId, menuLevelOneInfo, messageBody,
														SUBMENU_REQUEST_RECEIVED);
												generateUserBalanceReq(menuLevelOneInfo);

												// send to ocs and put in
												// menuLevelTwo
												menuLevelTwo.put(sessionId, menuLevelOneInfo);
											}
										}

									}
									if (!(menuLevelOne.containsKey(sessionId)) && !(ragMainMenu.containsKey(sessionId))
											&& !(ragSubMenu.containsKey(sessionId))) {
										// J4U Flow
										UssdMessage ussdMessage = getUSSDMessageModel(deliverSM, messageBody,
												sessionId);
										request = onReceivedUSSRAck(sessionId, ussdMessage);
										if (request != null) {
											request.setReferenceNumber(sessionId);
										}
									}

									break;
								}

							} else {
								// user not present in reduced ccr table notify
								// or inElgible
								UserInfo userInfoNotify = new UserInfo();
								userInfoNotify.setDestAddress(deliverSM.getDestination().getAddress());
								userInfoNotify.setMsisdn(msisdn);
								userInfoNotify.setMessageBody(messageBody);
								userInfoNotify.setTxId(new Utils().getTransactionID());
								request = (null != userInfo)
										? request = notifyUser(sessionId, userInfoNotify, messageBody,
												STATUS_USER_INELIGIBLE, userInfo.getLangCode(), false)
										: notifyUser(sessionId, userInfoNotify, messageBody, STATUS_USER_NOT_FOUND, 2,
												true); // sending default langCode french (2) and eligiblity (true)
														// assumimg it's J4U eligible
							}
						} else {
							// reject
							LOG.error(msisdn + " User input is not valid");
							UserInfo userInfo = lookUpDAO.getUserInfo(msisdn);
							if (null == userInfo) {
								userInfo = new UserInfo();
								userInfo.setLangCode("1"); // default french
							}
							userInfo.setDestAddress(deliverSM.getDestination().getAddress());
							userInfo.setMsisdn(msisdn);
							userInfo.setMessageBody(messageBody);
							userInfo.setTxId(new Utils().getTransactionID());
							request = sendErrorNotification(sessionId, userInfo, messageBody, STATUS_INVALID_REQUEST);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("Error Occurred..! " + e.getMessage());
				}
			}

			final DeliverSMResp deliverSMResp = new DeliverSMResp(deliverSM);
			if (ussdConnection.isBound()) {
				reentrantLock.lock();
				try {
					ussdConnection.sendResponse(deliverSMResp);
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					reentrantLock.unlock();
				}
			}

			if (request != null) {
				addToSendList(request);
			}
		}

		private void generateUserBalanceReq(UserInfo userInfo) throws Exception {
			JSONObject userBalJson = new JSONObject();
			userBalJson.put(MSISDN, userInfo.getMsisdn());
			userBalJson.put(USER_MSG_REF, userInfo.getUserMsgRef());
			userBalJson.put(TRANSACTION_ID, userInfo.getTxId());
			userBalJson.put(DEST_ADDRESS, userInfo.getDestAddress());
			userBalJson.put(SHORT_MSG, userInfo.getMessageBody());
			userBalJson.put(PRODUCT_TYPE, mlSubMenuProdTypeMenu.get(userInfo.getMessageBody()));
			userBalJson.put(LANG_CODE, userInfo.getLangCode());
			userBalJson.put(OFFER_REFRESH_FLAG, userInfo.getOfferRefreshFlag());
			UssdEventPublisher ussdEventPublisher = new UssdEventPublisher();
			ussdEventPublisher.addEvent(PropertiesLoader.getValue(OCS_QUERY_BAL_TOPIC), userBalJson.toString());
		}

		private UssdMessage getUSSDMessageModel(DeliverSM deliverSM, String messageBody, Integer sessionId)
				throws Exception {
			UssdMessage ussdMessage = new UssdMessage();
			ussdMessage.setSessionId(sessionId);
			ussdMessage.setSourceAddress(deliverSM.getSource().getAddress());
			ussdMessage.setDestinationAddress(deliverSM.getDestination().getAddress());
			ussdMessage.setMessageText(messageBody);
			ussdMessage.setDeliverSM(deliverSM);
			return ussdMessage;
		}

		private TransmitMessage notifyUser(int sessionId, UserInfo userInfo, String messageBody, String userStatus,
				int langCode, boolean isJFUEligible) throws Exception {
			String msisdn = userInfo.getMsisdn();
			LOG.info(msisdn + " :: " + userStatus);
			TransmitMessage message = null;
			messageListenerImpl.logInfo(sessionId, userInfo, messageBody, userStatus);
			InboundUssdMessage inboundUssdMessage = isJFUEligible ? messageListenerImpl.userNotFound(msisdn)
					: messageListenerImpl.userInEligible(langCode);
			if (inboundUssdMessage != null) {
				message = new TransmitMessage();
				message.setSourceAddress(userInfo.getDestAddress());
				message.setDestinationAddress(msisdn);
				message.setMessageText(inboundUssdMessage.getClobString());
				message.setTransactionId(userInfo.getTxId());
				message.setServiceOp(17);
				message.setReferenceNumber(sessionId);
				message.setMlFlag(userInfo.getMlFlag());
				message.setRandomFlag(userInfo.getRandomFlag());
			}
			return message;
		}

		private TransmitMessage provisionRewardAndNotifyUser(Integer sessionId, String messageBody, UserInfo userInfo)
				throws Exception {
			TransmitMessage message = null;
			messageListenerImpl.logInfo(sessionId, userInfo, messageBody, FINAL_REQUEST_RECEIVED);
			InboundUssdMessage inboundUssdMessage = messageListenerImpl.mlFinalReqReceived(messageBody, userInfo);
			if (inboundUssdMessage != null) {
				message = new TransmitMessage();
				message.setSourceAddress(userInfo.getDestAddress());
				message.setDestinationAddress(userInfo.getMsisdn());
				message.setMessageText(inboundUssdMessage.getClobString());
				message.setTransactionId(userInfo.getTxId());
				message.setReferenceNumber(userInfo.getUserMsgRef());
				message.setServiceOp(17);
				message.setMlFlag(userInfo.getMlFlag());
				message.setRandomFlag(userInfo.getRandomFlag());
				message.setSelProdId(inboundUssdMessage.getSelProdId());
			}

			return message;
		}

		private TransmitMessage subMenuMLRequest(Integer sessionId, String messageBody, UserInfo userInfo,
				boolean isRandom) throws Exception {
			TransmitMessage message = null;
			messageListenerImpl.logInfo(sessionId, userInfo, messageBody, SUBMENU_REQUEST_RECEIVED);
			InboundUssdMessage inboundUssdMessage = null;
			if (isRandom) {
				inboundUssdMessage = messageListenerImpl.subMenuReqReceived(messageBody, userInfo);
			} else {
				inboundUssdMessage = messageListenerImpl.subMenuReqReceivedForTgtUser(messageBody, userInfo);
			}

			if (inboundUssdMessage != null) {
				if (inboundUssdMessage.getIncomingLabel() == PROD_IDs_NOT_FOUND) {
					return notifyUser(sessionId, userInfo, messageBody, STATUS_PROD_IDS_INSUF, 2, true);
					// sending default langCode french (2) and eligiblity (true) assumimg it's J4U
					// eligible
				} else {
					message = new TransmitMessage();
					message.setDataCoding(1);
					message.setMessageEncoding(1);
					message.setSourceAddress(userInfo.getDestAddress());
					message.setDestinationAddress(userInfo.getMsisdn());
					message.setMessageText(inboundUssdMessage.getClobString());
					message.setTransactionId(userInfo.getTxId());
					message.setServiceOp(2);
					message.setReferenceNumber(sessionId);
					message.setMlFlag(userInfo.getMlFlag());
					message.setRandomFlag(userInfo.getRandomFlag());
					// setting 2 for ussd log table
					// status
					message.setProdIds(inboundUssdMessage.getProdIds());
				}
			}

			return message;
		}

		private TransmitMessage ragMainMenuRequest(Integer sessionId, String messageBody, UserInfo userInfo)
				throws Exception {
			TransmitMessage message = null;
			messageListenerImpl.logInfo(sessionId, userInfo, messageBody, SUBMENU_REQUEST_RECEIVED);
			InboundUssdMessage inboundUssdMessage = null;
			inboundUssdMessage = messageListenerImpl.ragMainMenuReqReceived(userInfo);
			if (inboundUssdMessage != null) {
				message = new TransmitMessage();
				message.setDataCoding(1);
				message.setMessageEncoding(1);
				message.setSourceAddress(userInfo.getDestAddress());
				message.setDestinationAddress(userInfo.getMsisdn());
				message.setMessageText(inboundUssdMessage.getClobString());
				message.setTransactionId(userInfo.getTxId());
				message.setServiceOp(2);
				message.setReferenceNumber(sessionId);
				message.setMlFlag(userInfo.getMlFlag());
				message.setRandomFlag(userInfo.getRandomFlag());
			}

			return message;
		}

		private TransmitMessage ragSubMenuRequest(Integer sessionId, String messageBody, UserInfo userInfo)
				throws Exception {
			TransmitMessage message = null;
			messageListenerImpl.logInfo(sessionId, userInfo, messageBody, SUBMENU_REQUEST_RECEIVED);
			InboundUssdMessage inboundUssdMessage = null;
			inboundUssdMessage = messageListenerImpl.ragSubMenuReqReceived(messageBody, userInfo);
			if (inboundUssdMessage != null) {
				message = new TransmitMessage();
				message.setDataCoding(1);
				message.setMessageEncoding(1);
				message.setSourceAddress(userInfo.getDestAddress());
				message.setDestinationAddress(userInfo.getMsisdn());
				message.setMessageText(inboundUssdMessage.getClobString());
				message.setTransactionId(userInfo.getTxId());
				message.setServiceOp(2);
				message.setReferenceNumber(sessionId);
				message.setMlFlag(userInfo.getMlFlag());
				message.setRandomFlag(userInfo.getRandomFlag());
			}

			return message;
		}

		private TransmitMessage ragOfferInfoRequest(Integer sessionId, String messageBody, UserInfo userInfo)
				throws Exception {
			TransmitMessage message = null;
			messageListenerImpl.logInfo(sessionId, userInfo, messageBody, SUBMENU_REQUEST_RECEIVED);
			InboundUssdMessage inboundUssdMessage = null;
			inboundUssdMessage = messageListenerImpl.ragOfferInfoReqReceived(messageBody, userInfo);
			if (inboundUssdMessage != null) {
				message = new TransmitMessage();
				message.setDataCoding(1);
				message.setMessageEncoding(1);
				message.setSourceAddress(userInfo.getDestAddress());
				message.setDestinationAddress(userInfo.getMsisdn());
				message.setMessageText(inboundUssdMessage.getClobString());
				message.setTransactionId(userInfo.getTxId());
				message.setServiceOp(2);
				message.setReferenceNumber(sessionId);
				message.setMlFlag(userInfo.getMlFlag());
				message.setRandomFlag(userInfo.getRandomFlag());
			}

			return message;
		}

		private TransmitMessage initialMLRequest(Integer sessionId, UserInfo userInfo, String messageBody)
				throws Exception {
			TransmitMessage message = null;
			messageListenerImpl.logInfo(sessionId, userInfo, messageBody, STATUS_REQUEST_RECEIVED);
			InboundUssdMessage inboundUssdMessage = null;
			inboundUssdMessage = messageListenerImpl
					.requestReceived(PropertiesLoader.getValue(USSD_ML_MAIN_MENU_TEMPLATE), userInfo.getLangCode());

			if (inboundUssdMessage != null) {
				message = new TransmitMessage();
				message.setDataCoding(1);
				message.setMessageEncoding(1);
				message.setSourceAddress(userInfo.getDestAddress());
				message.setDestinationAddress(userInfo.getMsisdn());
				message.setMessageText(inboundUssdMessage.getClobString());
				message.setTransactionId(userInfo.getTxId());
				message.setServiceOp(2);
				message.setReferenceNumber(sessionId);
				message.setMlFlag(userInfo.getMlFlag());
				message.setRandomFlag(userInfo.getRandomFlag());
			}
			return message;
		}

		private TransmitMessage sendErrorNotification(int sessionId, UserInfo userInfo, String messageBody,
				String status) throws Exception {
			TransmitMessage message = null;
			messageListenerImpl.logInfo(sessionId, userInfo, messageBody, status);
			InboundUssdMessage inboundUssdMessage = messageListenerImpl.getErrorMenu(userInfo.getLangCode());
			if (inboundUssdMessage != null) {
				message = new TransmitMessage();
				message.setSourceAddress(userInfo.getDestAddress());
				message.setDestinationAddress(userInfo.getMsisdn());
				message.setMessageText(inboundUssdMessage.getClobString());
				message.setTransactionId(userInfo.getTxId());
				message.setServiceOp(17);
				message.setReferenceNumber(sessionId);
				message.setMlFlag(userInfo.getMlFlag());
				message.setRandomFlag(userInfo.getRandomFlag());
			}
			return message;
		}

		private TransmitMessage onReceivedPSSR(int sessionId, UssdMessage request) {
			TransmitMessage message = null;
			String transactionId = new Utils().getTransactionID();
			request.setTransactionId(transactionId);
			messageListenerImpl.logInfo(sessionId, request, STATUS_REQUEST_RECEIVED);
			UssdSession session = new UssdSession(sessionId, UssdSession.MO);
			session.setTransactionId(transactionId);
			session.setMsisdn(request.getSourceAddress());
			if (null != activeSessions) {
				// if (!activeSessions.containsKey(sessionId)) {
				activeSessions.put(sessionId, session);
				LOG.debug("Ussd session created in Received PSSR ==> " + sessionId);
				/*
				 * } else { LOG.error("Duplicate Ussd Session Id ==> " + sessionId +
				 * " sent from the USSD Gateway"); }
				 */
			}

			UssdMessage response = messageListenerImpl.requestReceived(request, session);
			if (response != null) {
				message = new TransmitMessage();
				message.setDataCoding(1);
				message.setMessageEncoding(1);
				message.setSourceAddress(request.getDestinationAddress());
				message.setDestinationAddress(request.getSourceAddress());
				message.setMessageText(response.getMessageText());
				message.setTransactionId(transactionId);
				message.setProdIds(response.getProdIds());
				switch (response.getMessageType()) {
				case UssdMessage.NOTIFY:
					message.setServiceOp(17);
					break;
				case UssdMessage.REQUEST:
					message.setServiceOp(2);
					break;
				}
			}

			session.incrementSequenceNumber();

			if (null != response && response.getMessageType() == UssdMessage.NOTIFY) {
				LOG.debug("Removed UssdMessage NOTIFY Request:" + sessionId);
				activeSessions.remove(sessionId);
			}
			return message;
		}

		private TransmitMessage onReceivedUSSRAck(int sessionId, UssdMessage request) {
			TransmitMessage message = null;
			UssdSession session = null;
			LOG.debug("Source address within onRecceived USSR ACk:::"
					+ (null != request ? request.getSourceAddress() : "Request is empty"));
			if (null != activeSessions) {
				// LOG.debug("Actived Sessions in Outgoing inside
				// ReceivedUSSRAck:::" + activeSessions.get(sessionId));
				session = activeSessions.get(sessionId);
			}
			if (session == null) {
				LOG.warn("Could not find session id-" + sessionId + " in the actived sessions map");
				return null;
			}

			String transactionId = session.getTransactionId();

			if (null != transactionId) {
				request.setTransactionId(transactionId);
				messageListenerImpl.logInfo(sessionId, request, STATUS_USER_RESPONSE_RECEIVED);
				UssdMessage response = messageListenerImpl.responseReceived(request, session);
				if (response != null) {
					message = new TransmitMessage();
					message.setSourceAddress(request.getDestinationAddress());
					message.setDestinationAddress(request.getSourceAddress());
					message.setMessageText(response.getMessageText());
					message.setTransactionId(transactionId);
					message.setSelProdId(response.getSelProdId());
					switch (response.getMessageType()) {
					case UssdMessage.NOTIFY:
						// As requested by DRC, we send 17 for both inbound and
						// outbound to end the session.
						message.setServiceOp(17);
						break;
					case UssdMessage.REQUEST:
						message.setServiceOp(2);
						break;
					}
				} else {
					LOG.warn("Response from messageListener.responseReceived is null");
				}
				session.incrementSequenceNumber();
				if (null != response && response.getMessageType() == UssdMessage.NOTIFY) {
					LOG.debug("Remove USSRAck NOTIFY Request:" + sessionId);
					activeSessions.remove(sessionId);
				}
			} else {
				LOG.warn("Transaction ID was not found for session, returning null message");
			}
			return message;
		}

		@Override
		public void queryLink(Connection source, EnquireLink el) {
			LOG.debug("Enquire Link request received.");
			try {
				SMPPResponse response = (SMPPResponse) ussdConnection.newInstance(SMPPPacket.ENQUIRE_LINK_RESP);
				response.setCommandStatus(ESME_ROK);
				response.setSequenceNum(el.getSequenceNum());
				ussdConnection.sendResponse(response);
				LOG.debug("Enquire Link response sent.");
			} catch (Exception e) {
				LOG.error("Problem sending the enquire Link response.", e);
			}
		}

		@Override
		public void queryLinkResponse(Connection source, EnquireLinkResp elr) {
			LOG.debug("Enquire Link response received.");
			EnquiryLinkTime.enquiryResponseRecieved = true;
			EnquiryLinkTime.enquireLinkResponseRecievedTime.set(System.currentTimeMillis() - 100);
		}

		// Called when a bind response packet is received.
		@Override
		public void bindResponse(Connection source, BindResp br) {
			try {
				reentrantLock.lock();
				// on exiting this block, we're sure that
				// the main thread is now sitting in the wait
				// call, awaiting the unbind request.
				LOG.info("Bind response received.");

				if (br.getCommandStatus() == 0) {
					LOG.info("Successfully bound. Awaiting messages..");
				} else {
					LOG.info("Bind did not succeed! !Error! status = " + br.getCommandStatus());
					try {
						ussdConnection.closeLink();
					} catch (IOException x) {
						LOG.info("IOException closing link:\n" + x.toString());
					}
				}
				isConnected.signal();
			} catch (Throwable e) {
				LOG.error("Problem receiving the bind response.");
			} finally {
				reentrantLock.unlock();
			}
		}

		// This method is called when the SMSC sends an unbind request to our
		// receiver. We must acknowledge it and terminate gracefully..
		@Override
		public void unbind(Connection source, Unbind ubd) {
			LOG.info("SMSC requested unbind. Acknowledging..");
			try {
				// SMSC requests unbind..
				UnbindResp ubr = new UnbindResp(ubd);
				ussdConnection.sendResponse(ubr);
			} catch (IOException x) {
				LOG.info("IOException while acking unbind: " + x.toString());
			}
		}

		// This method is called when the SMSC responds to an unbind request we
		// sent
		// to it..it signals that we can shut down the network connection and
		// terminate our application..
		@Override
		public void unbindResponse(Connection source, UnbindResp ubr) {

			try {
				reentrantLock.lock();
				int st = ubr.getCommandStatus();

				if (st == 0) {
					LOG.info("Successfully unbound.");
				} else {
					LOG.info("Unbind response: !Error! status = " + st);
				}
				isConnected.signal();
			} catch (Throwable e) {
				LOG.error("Problem receiving the unbind response.");
			} finally {
				reentrantLock.unlock();
			}
		}

		// this method is called when the receiver thread is exiting normally.
		@Override
		public void receiverExit(Connection source, ReceiverExitEvent ev) {
			if (ev.getReason() == ReceiverExitEvent.BIND_TIMEOUT) {
				LOG.info("Bind timed out waiting for response.");
			}
			LOG.info("Receiver thread has exited.");
			connectionReset = true;
		}

		// this method is called when the receiver thread exits due to an
		// exception in the thread...
		@Override
		public void receiverExitException(Connection source, ReceiverExitEvent ev) {
			LOG.info("Receiver thread exited abnormally. The following exception was thrown:\n"
					+ ev.getException().toString());
			connectionReset = true;
		}
	}

	class TimeoutHandler implements Runnable {
		public void run() {
			while (isClearActiveSession) {
				try {
					Set<Integer> sessionIdSet = activeSessions.keySet();
					Iterator<Integer> iter = sessionIdSet.iterator();
					while (iter.hasNext()) {
						Integer key = iter.next();
						UssdSession session = activeSessions.get(key);
						if (System.currentTimeMillis() - session.getCreatedTime() >= 60000) {
							activeSessions.remove(key);
							LOG.debug("Session removed for the key :: " + key);
							messageListenerImpl.sessionTimedout(key, session);
						}
					}

					TimeUnit.MILLISECONDS.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}
	}

	class RetryConnection implements Runnable {

		@Override
		public void run() {
			LOG.debug("******* Into Run Method of RetryConnection *******");
			if (EnquiryLinkTime.retryCount.get() > retryCount) {
				LOG.warn("Exiting Retry Worker after Retrying " + retryCount + " times ");
				shutdown();
				try {
					retryExcService.awaitTermination(5, TimeUnit.SECONDS);
					throw new Exception("Exiting Retry Worker after Retrying " + retryCount + " times");
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
				}

			} else if ((EnquiryLinkTime.enquiryRequestSent && verifyToRestart()) || connectionReset) {
				LOG.debug("Retrying connection for " + EnquiryLinkTime.retryCount.get() + " time :: connectionReset => "
						+ connectionReset);
				try {
					unbind();
				} catch (UssdConnectionException e1) {
					e1.printStackTrace();
				}
				try {
					Thread.sleep(10000);
					bind();
					if (getConnectionStatus() == ACTIVE) {
						EnquiryLinkTime.retryCount.set(0);
					} else {
						EnquiryLinkTime.retryCount.incrementAndGet();
					}
				} catch (Exception e) {
					LOG.error("Error during Retry-->> : " + e.getMessage(), e);
				}
			}
		}
	}

	public void shutdown() {
		try {
			clearActiveSession();
			if (messageTransmitterExecutorService != null) {
				messageTransmitterExecutorService.shutdown();
			}
			if (removeInactiveSessionsService != null) {
				removeInactiveSessionsService.shutdown();
			}
			if (retryExcService != null) {
				retryExcService.shutdownNow();
			}
			if (keepAliveExecutorService != null) {
				keepAliveExecutorService.shutdown();
				keepAliveExecutorService.awaitTermination(5, TimeUnit.SECONDS);
			}
		} catch (InterruptedException e) {
			LOG.error("Exception in making the Keep Alive Thread to wait", e);
		} catch (Exception e) {
			LOG.error("Exception while shutting down the keep alivers", e);
		} finally {
			isClearActiveSession = false;
			if (activeSessions != null) {
				activeSessions.clear();
			}
		}
	}

	private void clearActiveSession() throws Exception {
		messageListenerImpl.cleanUp();
	}

	public int getConnectionStatus() {
		return connectionStatus;
	}

	public void setConnectionStatus(int connectionStatus) {
		this.connectionStatus = connectionStatus;
	}

	public void setMessageListener(MessageListenerImpl messageListenerImpl) {
		this.messageListenerImpl = messageListenerImpl;
	}

}