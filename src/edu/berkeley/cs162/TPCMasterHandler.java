/**
 * Handle TPC connections over a socket interface
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
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 *
 */
public class TPCMasterHandler implements NetworkHandler {
	private KVServer kvServer = null;
	private ThreadPool threadpool = null;
	private TPCLog tpcLog = null;
	
	private long slaveID = -1;
	
	// Used to handle the "ignoreNext" message
	private boolean ignoreNext = false;
	
	
	// States carried from the first to the second phase of a 2PC operation
	private KVMessage originalMessage = null;
	private boolean aborted = true;	
	
	private boolean overwrite = false;
	private String placeHolderInCaseOfAbort = null;
	
	public TPCMasterHandler(KVServer keyserver) {
		this(keyserver, 1);
	}

	public TPCMasterHandler(KVServer keyserver, long slaveID) {
		this.kvServer = keyserver;
		this.slaveID = slaveID;
		threadpool = new ThreadPool(1);
	}

	public TPCMasterHandler(KVServer kvServer, long slaveID, int connections) {
		this.kvServer = kvServer;
		this.slaveID = slaveID;
		threadpool = new ThreadPool(connections);
	}

	private class ClientHandler implements Runnable {
		private KVServer keyserver = null;
		private Socket client = null;

		private void closeConn() {
			try {
				client.close();
			} catch (IOException e) {
			}
		}
		
		@Override
		public void run() {
			// Receive message from client
			// Implement me
			KVMessage msg = null;
			try {
				msg = new KVMessage(client.getInputStream());
			} catch (IOException e){
				return;
			}catch (KVException e){
				return;
			}

			// Parse the message and do stuff 
			String key = msg.getKey();
			
			try {
				System.out.println("Received" + msg.toXML());
			} catch (KVException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			if (msg.getMsgType().equals("putreq")) {
				handlePut(msg, key);
				tpcLog.appendAndFlush(msg);
			}
			else if (msg.getMsgType().equals("getreq")) {
				handleGet(msg, key);
				tpcLog.appendAndFlush(msg);
			}
			else if (msg.getMsgType().equals("delreq")) {
				handleDel(msg, key);
				tpcLog.appendAndFlush(msg);
			} 
			else if (msg.getMsgType().equals("ignoreNext")) {
				// Set ignoreNext to true. PUT and DEL handlers know what to do.
				ignoreNext = true;
				// Send back an acknowledgment
				KVMessage response = null;
				try {
					response = new KVMessage("resp", "Success");
					response.setTpcOpId(msg.getTpcOpId());
					response.sendMessage(client);
				} catch (KVException e) {
					e.printStackTrace();
				}

			}
			else if (msg.getMsgType().equals("commit") || msg.getMsgType().equals("abort")) {
				// Check in TPCLog for the case when SlaveServer is restarted
				// Implement me	
				handleMasterResponse(msg, originalMessage, aborted);
				tpcLog.appendAndFlush(msg);
				
				sendACK(client, msg.getTpcOpId());
				// Reset state
				overwrite = false;
				ignoreNext = false;
				placeHolderInCaseOfAbort = null;
				aborted = false;
			}
			
			// Finally, close the connection
			closeConn();
		}
				
		private void handlePut(KVMessage msg, String key) {
			AutoGrader.agTPCPutStarted(slaveID, msg, key);
			if(ignoreNext){
				sendACK(client, msg.getTpcOpId());
				AutoGrader.agGetFinished(slaveID);
				return;
			}
			// Store for use in the second phase
			originalMessage = new KVMessage(msg);
			
			try {
				placeHolderInCaseOfAbort = kvServer.get(key);
				if(placeHolderInCaseOfAbort != null){
					overwrite = true;
				}
			} catch (KVException e) {
				//Do Nothing
			}

			try{
				kvServer.put(key, msg.getValue());
			} catch (KVException e) {
				aborted = true;
				sendAbort(client, msg.getTpcOpId());
				AutoGrader.agTPCPutFinished(slaveID, msg, key);
				return;
			}
			sendReady(client, msg.getTpcOpId());
			AutoGrader.agTPCPutFinished(slaveID, msg, key);
		}
		
 		private void handleGet(KVMessage msg, String key) {
 			AutoGrader.agGetStarted(slaveID);
			String toReturn = null;
 			try {
 				toReturn = kvServer.get(key);
				
			} catch (KVException e) {
				//Do Nothing
			}
 			
 			//Send Message
 			KVMessage resp;
			try {
				resp = new KVMessage("resp");
				resp.setValue(toReturn);
	 			resp.setKey(msg.getKey());
	 			resp.sendMessage(client);
			} catch (KVException e) {
				e.printStackTrace();
			}
			AutoGrader.agGetFinished(slaveID);
		}
		
		private void handleDel(KVMessage msg, String key) {
			AutoGrader.agTPCDelStarted(slaveID, msg, key);

			// Store for use in the second phase
			originalMessage = new KVMessage(msg);
			
			if(ignoreNext){
				sendACK(client, msg.getTpcOpId());
				AutoGrader.agGetFinished(slaveID);
				return;
			}
			
			//Get Original value and store in case of abort
			try {
				placeHolderInCaseOfAbort = kvServer.get(key);
				if(placeHolderInCaseOfAbort != null){
					overwrite = true;
				}
			} catch (KVException e) {
				//Do Nothing
			}
			
			try {
				kvServer.del(key);
				
			} catch (KVException e) {
				aborted = true;
				sendAbort(client, msg.getTpcOpId());
			}

			AutoGrader.agTPCDelFinished(slaveID, msg, key);
		}

		/**
		 * Second phase of 2PC
		 * 
		 * @param masterResp Global decision taken by the master
		 * @param origMsg Message from the actual client (received via the coordinator/master)
		 * @param origAborted Did this slave server abort it in the first phase 
		 */
		private void handleMasterResponse(KVMessage masterResp, KVMessage origMsg, boolean origAborted) {
			AutoGrader.agSecondPhaseStarted(slaveID, origMsg, origAborted);
			
			if(origAborted){
				sendACK(client, masterResp.getTpcOpId());
			} else if( masterResp.getMsgType().equals("abort")) {
				try{
					if( origMsg.getMsgType().equals("put")){
						if(overwrite){
							kvServer.put(origMsg.getKey(), placeHolderInCaseOfAbort);
						} else{
							kvServer.del(origMsg.getKey());
						}
					}
					if( origMsg.getMsgType().equals("del")){
							kvServer.put(origMsg.getKey(), placeHolderInCaseOfAbort);
					}
				} catch (KVException e){
					
				}
			} else if( masterResp.getMsgType().equals("commit")) {
				
			}
			
			AutoGrader.agSecondPhaseFinished(slaveID, origMsg, origAborted);
		}

		public ClientHandler(KVServer keyserver, Socket client) {
			this.keyserver = keyserver;
			this.client = client;
		}
	}

	public void sendACK(Socket aClient, String tpcID){
		try {
			KVMessage ack = new KVMessage("ack");
			ack.setTpcOpId(tpcID);
			ack.sendMessage(aClient);
		} catch (KVException e) {
			e.printStackTrace();
		}
	}
	
	public void sendAbort(Socket aClient, String tpcID){
		try {
			KVMessage ack = new KVMessage("abort");
			ack.setTpcOpId(tpcID);
			ack.sendMessage(aClient);
		} catch (KVException e) {
			e.printStackTrace();
		}
	}
	public void sendReady(Socket aClient, String tpcID){
		try {
			KVMessage ready = new KVMessage("ready");
			ready.setTpcOpId(tpcID);
			ready.sendMessage(aClient);
		} catch (KVException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void handle(Socket client) throws IOException {
		AutoGrader.agReceivedTPCRequest(slaveID);
		Runnable r = new ClientHandler(kvServer, client);
		try {
			threadpool.addToQueue(r);
		} catch (InterruptedException e) {
			// TODO: HANDLE ERROR
			return;
		}		
		AutoGrader.agFinishedTPCRequest(slaveID);
	}

	/**
	 * Set TPCLog after it has been rebuilt
	 * @param tpcLog
	 */
	public void setTPCLog(TPCLog tpcLog) {
		this.tpcLog  = tpcLog;
	}

	/**
	 * Registers the slave server with the coordinator
	 * 
	 * @param masterHostName
	 * @param servr KVServer used by this slave server (contains the hostName and a random port)
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws KVException
	 */
	public void registerWithMaster(String masterHostName, SocketServer server) throws UnknownHostException, IOException, KVException {
		AutoGrader.agRegistrationStarted(slaveID);
		
		Socket master = new Socket(masterHostName, 9090);
		KVMessage regMessage = new KVMessage("register", slaveID + "@" + server.getHostname() + ":" + server.getPort());
		regMessage.sendMessage(master);
		
		// Receive master response. 
		// Response should always be success, except for Exceptions. Throw away.
		new KVMessage(master.getInputStream());
		
		master.close();
		AutoGrader.agRegistrationFinished(slaveID);
	}
}
