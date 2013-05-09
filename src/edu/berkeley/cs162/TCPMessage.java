package edu.berkeley.cs162;

import java.io.Serializable;

public class TCPMessage extends KVMessage implements Serializable {
	//this is in KVMessage so I put it here too
	private static final long serialVersionUID = 6473128480951955693L;

	private String msgType = null;
	private String key = null;
	private String value = null;
	private String message = null;
	private String tcpOpId = null;
	//isputresp?
	
	public TCPMessage( String msgType){
		this.msgType = msgType;
	}
	
	public TCPMessage(String msgType, String key, String value, String message, String tcpOpId){
		this.msgType = msgType;
		this.key = key;
		this.value = value;
		this.message = message;
		this.tcpOpId = tcpOpId;
	}
}
