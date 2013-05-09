/**
 * XML Parsing library for the key-value store
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 * 
 * Copyright (c) 2012, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;


/**
 * This is the object that is used to generate messages the XML based messages 
 * for communication between clients and servers. 
 */
public class KVMessage implements Serializable {
	
	private static final long serialVersionUID = 6473128480951955693L;
	
	private String msgType = null;
	private String key = null;
	private String value = null;
	private String message = null;
    private String tpcOpId = null;    
    ArrayList<String> msgTypes= new ArrayList<String>(Arrays.asList(
    		"putreq",
    		"getreq",
    		"delreq",
    		"resp",
    		"register",
    		"ack",
    		"ignoreNext",
    		"abort", 
    		"ready",
    		"commit"));
	public final String getKey() {
		return key;
	}

	public final void setKey(String key) {
		this.key = key;
	}

	public final String getValue() {
		return value;
	}

	public final void setValue(String value) {
		this.value = value;
	}

	public final String getMessage() {
		return message;
	}

	public final void setMessage(String message) {
		this.message = message;
	}

	public String getMsgType() {
		return msgType;
	}
	
	public void setMsgType(String msgType) {
		this.msgType = msgType;
	}

	public String getTpcOpId() {
		return tpcOpId;
	}

	public void setTpcOpId(String tpcOpId) {
		this.tpcOpId = tpcOpId;
	}

	/* Solution from http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html */
	private class NoCloseInputStream extends FilterInputStream {
	    public NoCloseInputStream(InputStream in) {
	        super(in);
	    }
	    
	    public void close() {} // ignore close
	}
	
	/***
	 * 
	 * @param msgType
	 * @throws KVException of type "resp" with message "Message format incorrect" if msgType is unknown
	 */
	public KVMessage(String msgType) throws KVException {
		if (msgTypes.contains(msgType)){
			this.msgType = msgType;
		}else{
			throw new KVException (new KVMessage("resp", "Unknown Error: Message format incorrect"));
		}
	}
	
	public KVMessage(String msgType, String message) throws KVException {
		this(msgType);
		this.message = message;
		
	}
	
	 /***
     * Parse KVMessage from incoming network connection
     * @param sock
     * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
     * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
     * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
     * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
     */
	public KVMessage(InputStream input) throws KVException {
		/*
		 * After Spring 2013, this will be taken out in favor of passing in the socket directly.
		 */

		Document newDoc= null; 
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db;
		//CREATE A NEW DOCUMENT BUILDER
		try {
			db = dbf.newDocumentBuilder();
		}catch(ParserConfigurationException e){
			KVMessage errorMsg = new KVMessage("resp", "Unknown Error: DocumentBuilder");;
			throw new KVException (errorMsg);
		}
		NoCloseInputStream noCloseInput = new NoCloseInputStream(input);
		//TRY TO PARSE THE INPUT STREAM
		try{
			newDoc = db.parse(noCloseInput);
			newDoc.setXmlStandalone(true);
		}catch(IOException e){
			KVMessage ioError = new KVMessage( "resp" , "Network Error: Could not receive data");
			throw new KVException(ioError);

		} catch (SAXException e) {
			// TODO Auto-generated catch block
			KVMessage saxError = new KVMessage ("resp", "XML Error: Received unparseable message");
			throw new KVException(saxError);

		}
		    Element rootElement = newDoc.getDocumentElement();
		    String msgType = rootElement.getAttribute("type");
		    if (msgType.equals("putreq")){
		    	Node incomingKey = rootElement.getFirstChild();
		    	Node incomingVal = rootElement.getLastChild();
		    	
		    	//check for null keys & vals & null textContents
		    	if (incomingKey == null || incomingKey.getTextContent() == null || incomingVal == null || incomingVal.getTextContent()== null){
		    		KVMessage thisMsg = new KVMessage( "resp", "XML Error: Received unparseable message");
		    		throw new KVException(thisMsg);
		    	}
		    
		    	//set up the pieces
		    	this.msgType = msgType;
		    	this.key = incomingKey.getTextContent();
		    	this.value = incomingVal.getTextContent();
		    	
		    }else if (msgType.equals("getreq")){ 
		    	Node incomingKey = rootElement.getFirstChild();
		    	if (incomingKey == null || incomingKey.getTextContent()==null){
		    		KVMessage thisErrorMsg = new KVMessage ("resp", "XML Error: Received unparseable message");
		    		throw new KVException(thisErrorMsg);
		    	}
		    	this.msgType = msgType;
		    	this.key = incomingKey.getTextContent();
		    	
		    	
		    }else if (msgType.equals("resp")){
		    	Element rootChild = (Element) rootElement.getFirstChild();
		    	if (rootChild.getTagName().equals("Key")){
		    		Node myKey = rootChild;
		    		Node myVal = (Element) rootElement.getLastChild();
		    		if (myKey.getTextContent() == null || myVal.getTextContent()== null){
		    			throw new KVException( new KVMessage ("resp", "XML Error: Received unparseable message"));
		    		}
		    		this.msgType = msgType;
		    		this.key = myKey.getTextContent();
		    		this.value = myVal.getTextContent();
		    	}else{
		    		this.msgType = msgType;
		    		this.message = rootChild.getTextContent();
		    	}
		    	
		    }else if(msgType.equals("delreq")){
		    	Node keyToDelete = rootElement.getFirstChild();
		    	if (keyToDelete == null || keyToDelete.getTextContent() == null){
		    		throw new KVException (new KVMessage ("resp", "XML Error: Received unparseable message"));
		    		
		    	}
		    	this.msgType = msgType;
		    	this.key = keyToDelete.getTextContent();
		    }else if (msgType.equals("commit")){
		    	Node tpcOpIdNode = rootElement.getFirstChild();
		    	this.tpcOpId = tpcOpIdNode.getTextContent();
		    	this.msgType = msgType;
		    	
		    }else if (msgType.equals("abort")){
		    	//aborts either pass in an error or a message
		    	Node firstChild = rootElement.getFirstChild();
		    	if (firstChild.getNodeName().equals("Message")){
		    		Node tpcOpIdNode = rootElement.getLastChild();
		    		this.tpcOpId = tpcOpIdNode.getTextContent();
		    		this.message = firstChild.getTextContent();
		    	}else { // this is an error
		    		this.tpcOpId = firstChild.getTextContent();
		    	}
		    }else if (msgType.equals("ready")){
		    	Node tpcOpIdNode = rootElement.getFirstChild();
		    	this.tpcOpId = tpcOpIdNode.getTextContent();
		    	this.msgType = msgType;
		    }else if (msgType.equals("ack")){
		    	Node tpcOpIdNode = rootElement.getFirstChild();
		    	this.tpcOpId = tpcOpIdNode.getTextContent();
		    	this.msgType = msgType;
		    }else if (msgType.equals("ignoreNext")){
		    	this.msgType = msgType;
		    }else if (msgType.equals("register")){
		    	Node msgNode = rootElement.getFirstChild();
		    	this.message = msgNode.getTextContent();
		    	this.msgType = msgType;
		    }else{
		    	//invalid message type
		    	throw new KVException(new KVMessage("resp", "XML Error: Received unparseable message"));
		    }
	}
	
	/**
	 * 
	 * @param sock Socket to receive from
	 * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
	 * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
	 * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
	 * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
	 */
	public KVMessage(Socket sock) throws KVException {
		
	}

	/**
	 * 
	 * @param sock Socket to receive from
	 * @param timeout Give up after timeout milliseconds
	 * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
	 * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
	 * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
	 * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
	 */
	public KVMessage(Socket sock, int timeout) throws KVException {
	     // TODO: implement me
	}
	
	/**
	 * Copy constructor
	 * 
	 * @param kvm
	 */
	public KVMessage(KVMessage kvm) {
		this.msgType = kvm.msgType;
		this.key = kvm.key;
		this.value = kvm.value;
		this.message = kvm.message;
		this.tpcOpId = kvm.tpcOpId;
	}

	/**
	 * Generate the XML representation for this message.
	 * @return the XML String
	 * @throws KVException if not enough data is available to generate a valid KV XML message
	 */
	public String toXML() throws KVException {
		String xmlString;
		Text text;
		KVMessage xmlError =new KVMessage("resp", "XML Error: Received unparseable message");
		     
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;
		Document newDoc = null; 
		//try to make a new Doc builder which is the XML base
		try{
		db = dbf.newDocumentBuilder();
		newDoc = db.newDocument();
		newDoc.setXmlStandalone(true);
		}catch (ParserConfigurationException e){
		KVMessage dbMessage = new KVMessage("Unknown Error: toXML Document Building error");
		throw new KVException(dbMessage);
		}

		//now to make the XML Tree! 
		//MAKE THE ROOT ELEMENT the KVMessage
		Element rootElement = newDoc.createElement("KVMessage");
		//ADD THE ROOT TO THE XML
		//SET THE TYPE
		rootElement.setAttribute("type", msgType);
		newDoc.appendChild(rootElement);


		//HERE COMES THE FUN
		//DO THE MESSAGE BUSINESS
		if (!msgTypes.contains(msgType)){
			throw new KVException (new KVMessage("resp", "XML Error: Received unparseable message"));
		}
		if (msgType == "putreq"){
			if (key !=null && value !=null){
				Element keyChild = newDoc.createElement("Key");
				keyChild.setTextContent(key);
				rootElement.appendChild(keyChild);

				Element valueChild = newDoc.createElement("Value");
				valueChild.setTextContent(value);
				rootElement.appendChild(valueChild);
			}else{
				throw new KVException (new KVMessage("resp", "XML Error: Received unparseable message"));
		}

		if (msgType == "commit" || msgType == "ack"){
			Element id = newDoc.createElement("TPCOpId");
			id.setTextContent(this.message);
			rootElement.appendChild(id);
		}

		if (msgType == "abort"){
			if (this.message != null){
				Element messageElem = newDoc.createElement("Message");
				messageElem.setTextContent(this.message);
				rootElement.appendChild(messageElem);
			}else{
				Element tpIdElem = newDoc.createElement("TPCOpId");
				tpIdElem.setTextContent(this.tpcOpId);
				rootElement.appendChild(tpIdElem);
				}
		}
		if (msgType == "register"){
			Element message = newDoc.createElement("Message");
			message.setTextContent(this.message);
			rootElement.appendChild(message);
		}
		if (msgType == "ignoreNext"){
			//dont do anything? 
		}
		if (msgType == "ready"){
			Element tpId = newDoc.createElement("TPCOpId");
			tpId.setTextContent(this.tpcOpId);
			rootElement.appendChild(tpId);
		}

		}
		if (msgType == "resp"){
			//if resp has K&V
			if (key !=null && value != null){
				Element keyChild = newDoc.createElement("Key");
				keyChild.setTextContent(key);
				rootElement.appendChild(keyChild);

				Element valueChild = newDoc.createElement("Value");
				valueChild.setTextContent(value);
				rootElement.appendChild(valueChild);
			}
			//if resp has no K&V
			else if (key == null && value ==null){
				Element messageChild = newDoc.createElement("Message");
				messageChild.setTextContent(message);
				rootElement.appendChild(messageChild);
			}else{
				throw new KVException (new KVMessage ("resp","XML Error: Received unparseable message" ));
			}
		}
		if (msgType == "getreq" ){
			if (key !=null){
				Element keyChild = newDoc.createElement("Key");
				keyChild.setTextContent(key);
				rootElement.appendChild(keyChild);
			}else{
				throw new KVException (new KVMessage("resp", "XML Error: Received unparseable message"));
			}
		}

		if (msgType == "delreq"){
			if (key !=null){
				Element keyChild = newDoc.createElement("Key");
				keyChild.setTextContent(key);
				rootElement.appendChild(keyChild);

			}
		}



		//XML Tree done
		// now need to transform this to output it
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer arnold = null;

		//try to make a new transformer
		try{
			arnold = tf.newTransformer();
		}catch (TransformerConfigurationException e){
			throw new KVException(xmlError);
		}



		// now take the string from the xml
		StringWriter stringwriter = new StringWriter();
		StreamResult dst = new StreamResult(stringwriter);

		DOMSource src = new DOMSource(newDoc);
		//try to tranfsorm xml src to dst
		try{
			arnold.transform(src, dst);
		}catch (TransformerException e){
			throw new KVException(xmlError);
		}
		//get the string from the string writer
		return stringwriter.toString();

	}
	
	public void sendMessage(Socket sock) throws KVException {
		String xml = this.toXML();
		Writer output;
		try{
			output = new OutputStreamWriter(sock.getOutputStream());
			output.write(xml);
			output.flush();
		}catch(IOException e){
			//KVMessage
			KVMessage ioError = new KVMessage("resp", "Network Error: Could not send data");
			throw new KVException (ioError);
		}

		try {
			sock.shutdownOutput();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			KVMessage socketError = new KVMessage("resp", "Network Error: Could not send data");
			throw new KVException(socketError);

		}
	}
	
	public void sendMessage(Socket sock, int timeout) throws KVException {
		/*
		 * As was pointed out, setting a timeout when sending the message (while would still technically work),
		 * is a bit silly. As such, this method will be taken out at the end of Spring 2013.
		 */
		// TODO: optional implement me
	}
}
