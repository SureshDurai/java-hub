package com.javahub.keepalive;


public class KeepConnectionAlive implements Runnable {

	private Connection Connection;

	public KeepConnectionAlive(Connection ussdConnection) {
		this.ussdConnection = ussdConnection;
	}

	public void run() {
		try {
			LOG.debug("Sending Enquiry Link Request");
			ussdConnection.sendRequest(new EnquireLink());
			LOG.debug("Connection Status is::" + ussdConnection.getState());
			EnquiryLinkTime.enquiryRequestSent = true;
			EnquiryLinkTime.enquireLinkRequestTime.set(System.currentTimeMillis());
		} catch (Throwable t) {
			t.printStackTrace();
			LOG.error("Error in sending the Enquiry Link with thread-" + Thread.currentThread().getName(), t);
		}
	}
}