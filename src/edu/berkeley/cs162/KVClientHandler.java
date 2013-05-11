/**
 * Handle client connections over a socket interface
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
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import edu.berkeley.cs162.NetworkHandler;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections. 
 * It uses a threadpool to ensure that none of it's methods are blocking.
 *
 */
public class KVClientHandler implements NetworkHandler {
	private TPCMaster tpc_master = null;
	private ThreadPool threadpool = null;
	
	public KVClientHandler(TPCMaster kvServer) {
		initialize(kvServer, 1);
	}

	public KVClientHandler(TPCMaster kvServer, int connections) {
		initialize(kvServer, connections);
	}

	private void initialize(TPCMaster kvServer, int connections) {
		this.tpc_master = kvServer;
		threadpool = new ThreadPool(connections);	
	}
	

	private class ClientHandler implements Runnable {
		private TPCMaster tpc_master = null;
		private Socket client = null;
		
		@Override
		public void run() {			
		    //Check if mesgType is valid
		    KVMessage clientMessage = null;
		    try{
		    	clientMessage = new KVMessage(client.getInputStream());
		    } catch (KVException e){
		    	//If XML is invalid, we must send a fail response
		    	try {
					e.getMsg().sendMessage(client);
				} catch (KVException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		    	
		    } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		    //Finally if everything passes, we process the requests
		    /* ************* Put Request ******************* */
		    if(clientMessage.getMsgType().equals("putreq")){
		    	try {
					tpc_master.performTPCOperation(clientMessage, true);
				} catch (KVException e) {
					//Send fail response
					try {
						e.getMsg().sendMessage(client);
					} catch (KVException e1) {
						//Do nothing
					}
				}
		    	//Send success response
		    	KVMessage msg = null;
				try {
					msg = new KVMessage("resp","Success");
				} catch (KVException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
		    	try {
					msg.sendMessage(client);
				} catch (KVException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    /* ************* Get Request ******************* */
		    } else if(clientMessage.getMsgType().equals("getreq")){
		    	String valueReturned = null;
		    	
		    	try {
					valueReturned = tpc_master.handleGet(clientMessage);
				} catch (KVException e) {
					try {
						e.getMsg().sendMessage(client);
					} catch (KVException e1) {
						//Do nothing if send fails
					}
				}
		    	//Send success response
		    	KVMessage msg = null;
				try {
					msg = new KVMessage("resp","Success");
				} catch (KVException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		    	msg.setKey(clientMessage.getKey());
		    	msg.setValue(valueReturned);
		    	try {
					msg.sendMessage(client);
				} catch (KVException e) {
					
				}
		    /* ************* Del Request ******************* */
		    } else if(clientMessage.getMsgType().equals("delreq")){
		    	try {
					tpc_master.performTPCOperation(clientMessage, false);
				} catch (KVException e) {
					try {
						e.getMsg().sendMessage(client);
					} catch (KVException e1) {
						
					}
				}
		    	
		    	KVMessage msg = null;
				try {
					msg = new KVMessage("resp","Success");
				} catch (KVException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		    	try {
					msg.sendMessage(client);
				} catch (KVException e) {
					
				}
		    }
		}
		
		public ClientHandler(TPCMaster kvServer, Socket client) {
			this.tpc_master = kvServer;
			this.client = client;
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.berkeley.cs162.NetworkHandler#handle(java.net.Socket)
	 */
	@Override
	public void handle(Socket client) throws IOException {
		Runnable r = new ClientHandler(tpc_master, client);
		try {
			threadpool.addToQueue(r);
		} catch (InterruptedException e) {
			// Ignore this error
			return;
		}
	}
}
