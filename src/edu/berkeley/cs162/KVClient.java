/**
 * Client component for generating load for the KeyValue store. 
 * This is also used by the Master server to reach the slave nodes.
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

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;


/**
 * This class is used to communicate with (appropriately marshalling and unmarshalling) 
 * objects implementing the {@link KeyValueInterface}.
 *
 * @param <K> Java Generic type for the Key
 * @param <V> Java Generic type for the Value
 */
public class KVClient implements KeyValueInterface {

	private String server = null;
	private int port = 0;
	
	/**
	 * @param server is the DNS reference to the Key-Value server
	 * @param port is the port on which the Key-Value server is listening
	 */
	public KVClient(String server, int port) {
		this.server = server;
		this.port = port;
	}
	
	private Socket connectHost() throws KVException {
	    Socket toReturnSock = null;
		try {
			toReturnSock = new Socket(this.server, this.port);
		} catch (UnknownHostException e) {
			new KVException (new KVMessage("resp","Error: cannot connect to host"));
		} catch (IOException e) {
			
		}
		
		return toReturnSock;
	}
	
	private void closeHost(Socket sock) throws KVException {
	    // TODO: Implement Me!
	    try{
		    sock.close();
		}
		catch (Exception e){
			new KVException (new KVMessage("Error: cannot close socket"));
		}
	}
	
	public void put(String key, String value) throws KVException {
	    if(key == null || value == null)
	    	throw new KVException (new KVMessage("Error: null key or value"));
	    
	    	Socket sock = connectHost();
	    	
	    	KVMessage request = new KVMessage("putreq");
	    	request.setKey(key);
	    	request.setValue(value);
	    	request.sendMessage(sock);
	    	
	    	//Checkout the Response
	    	KVMessage response = null;
	    	InputStream is = null;
	    	try {
	    		is = sock.getInputStream();
			} catch (IOException e) {
				throw new KVException(new KVMessage("resp", "Network Error: Could not receive data"));
			}
	    	response = new KVMessage(is);
	    	
	    	if(response.getMessage().equals("IO Error")){
	    		closeHost(sock);
	    		throw new KVException(response);
	    	}
	    	closeHost(sock);
	    
	}

	public String get(String key) throws KVException {
	    if(key == null)
	    	throw new KVException (new KVMessage("Error: null"));
	    
	   
	    	Socket sock = connectHost();
	    	KVMessage mess = new KVMessage("getreq");
	    	mess.setKey(key);
	    	mess.sendMessage(sock);	 
	    	
	    	//Checkout the Response
	    	KVMessage response = null;
	    	InputStream is = null;
	    	try {
	    		is = sock.getInputStream();
			} catch (IOException e) {
				throw new KVException(new KVMessage("resp", "Network Error: Could not receive data"));
			}
	    	response = new KVMessage(is);
	    	
	    	 if (response.getValue() != null) {
	             closeHost(sock);
	 	    	return response.getValue();
	         }
	 	    throw new KVException(response);
		
	}
	
	public void del(String key) throws KVException {
	   	if(key == null)
	    	throw new KVException (new KVMessage("Error: null key in delete"));
	   
	    	Socket sock = connectHost();
	    	KVMessage mess = new KVMessage("delreq");
	    	mess.setKey(key);
	    	mess.sendMessage(sock);
	    	
	    	//Checkout the Response
	    	KVMessage response = null;
	    	InputStream is = null;
	    	try {
	    		is = sock.getInputStream();
				response = new KVMessage(is);
			} catch (IOException e) {
				e.printStackTrace();
			}
	    	
	    	if(response.getMessage().equals("Does not exist")){
	    		throw new KVException(response);
	    	}
	    
	   	closeHost(sock);
	}	
	
}
