/**
 * Master for Two-Phase Commits
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

public class TPCMaster{
	
	/**
	 * Implements NetworkHandler to handle registration requests from 
	 * SlaveServers.
	 * 
	 */
	private class TPCRegistrationHandler implements NetworkHandler {

		private ThreadPool threadpool = null;

		public TPCRegistrationHandler() {
			// Call the other constructor
			this(1);	
		}

		public TPCRegistrationHandler(int connections) {
			threadpool = new ThreadPool(connections);	
		}

		@Override
		public void handle(Socket client) throws IOException {
			System.out.println("Received Registration Request");
			Runnable r = new RegistrationHandler(client);
			try{
				threadpool.addToQueue(r);
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}
		
		private class RegistrationHandler implements Runnable {
			private Socket client = null;

			public RegistrationHandler(Socket client) {
				this.client = client;
			}

			@Override
			public void run() {
				System.out.println("Running Registration for SlaveServer");
				try{
					KVMessage resp = new KVMessage ("resp");
					try{
						KVMessage registrationMessage = new KVMessage(client.getInputStream());
						SlaveInfo toBeRegistered = new SlaveInfo (registrationMessage.getMessage());
						synchronized(slaveInfoList){
							int position = Collections.binarySearch(slaveInfoList, toBeRegistered);
							if (position >= 0){ //slaveInfo  is already in the list
								slaveInfoList.set(position, toBeRegistered);
								resp.setMessage("Successfully registered "+ registrationMessage.getMessage());
							}else{ //pos < 0 so the item is not present already
								//if there arent enough slaves there, add it
								if(slaveInfoList.size() < numSlaves){
									//make the position positive. 
									slaveInfoList.add(toBeRegistered);
									resp.setMessage("Successfully registered " + registrationMessage.getMessage());
								// if there are enough slaves, then replace it
								}else{
									slaveInfoList.set(position, toBeRegistered);
									resp.setMessage("Successfully registered "+ registrationMessage.getMessage());
								}
							}
							if (slaveInfoList.size() == numSlaves && !ready){
								ready = true;
								slaveInfoList.notifyAll();
							}
						}//synch
					}catch (KVException e){
						resp.setMessage("Registration unsuccessful");
					}catch(IOException e){
						resp.setMessage("Registration unsuccessful");

					}
					resp.sendMessage(client);
				}catch(KVException e){
					e.printStackTrace();
					//not sure what to do here?
				}
			}
		}	
	}
	
	/**
	 *  Data structure to maintain information about SlaveServers
	 *
	 */
	private class SlaveInfo implements Comparable<SlaveInfo> {
		// 64-bit globally unique ID of the SlaveServer
		private long slaveID = -1;
		// Name of the host this SlaveServer is running on
		private String hostName = null;
		// Port which SlaveServer is listening to
		private int port = -1;
		
		//keep track of socket and client
		//KVClient kvClient = null;
		private Socket slaveSock = null;

		/**
		 * 
		 * @param slaveInfo as "SlaveServerID@HostName:Port"
		 * @throws KVException
		 */
		public SlaveInfo(String slaveInfo) throws KVException {
			KVMessage unparseError = new KVMessage("resp", "Registration Error: received unparsable slave information");
			
			//parse those out
			String [] slaveInfoPieces = slaveInfo.split("[@:]");
			if (slaveInfoPieces.length != 3){
				throw new KVException(unparseError);
			}else if (slaveInfo.indexOf("@") == -1 || slaveInfo.indexOf(":")==-1){
				throw new KVException(unparseError);
			}else if (slaveInfo.split("@").length != 2 || slaveInfo.split(":").length != 2){
				throw new KVException (unparseError);
			}else{
				try{
					slaveID = Long.parseLong(slaveInfoPieces[0]);
					port = Integer.parseInt(slaveInfoPieces[2]);
				}catch (NumberFormatException e){
					throw new KVException(unparseError);
				}
				//set up the client with the host name
				hostName = slaveInfoPieces[1];
				//kvClient = new KVClient(hostName, port);
			}
		}
		
		public long getSlaveID() {
			return slaveID;
		}
		
		public String hostName() {
			return hostName;
		}
		
		public int port() {
			return port;
		}
		
		public Socket connectHost() throws KVException {
			try {
				slaveSock = new Socket(hostName, port);
			} catch (UnknownHostException e) {
				throw new KVException(new KVMessage("resp", "Network Error: Could not create socket"));
			} catch (IOException e) {
				throw new KVException(new KVMessage("resp", "Network Error: Could not connect"));
			}
			return slaveSock;
		}
		
		public void closeHost() throws KVException {
			try {
				slaveSock.close();
			} catch (IOException e) {
				throw new KVException(new KVMessage("resp", "Unknown Error: Error Closing Socket"));
			}
			slaveSock = null;
		}

		@Override
		public int compareTo(SlaveInfo arg0) {
			if (this.slaveID == arg0.slaveID){
				return 0;
			}else{
				return 1;
			}
		}
	}
	
	//Must keep track of the slaves
	//left side must be a list so you can use binarysearch in collections :)
	private List<SlaveInfo> slaveInfoList = new ArrayList<SlaveInfo>();
	
	//Keep trak if its ready
	private boolean ready = false;
	// Timeout value used during 2PC operations
	private static final int TIMEOUT_MILLISECONDS = 5000;
	
	// Cache stored in the Master/Coordinator Server
	private KVCache masterCache = new KVCache(100, 10);
	
	// Registration server that uses TPCRegistrationHandler
	private SocketServer regServer = null;

	// Number of slave servers in the system
	private int numSlaves = -1;
	
	// ID of the next 2PC operation
	private Long tpcOpId = 0L;
	
	/**
	 * Creates TPCMaster
	 * 
	 * @param numSlaves number of expected slave servers to register
	 * @throws Exception
	 */
	public TPCMaster(int numSlaves) {
		// Using SlaveInfos from command line just to get the expected number of SlaveServers 
		this.numSlaves = numSlaves;

		// Create registration server
		regServer = new SocketServer("localhost", 9090);
		
	}
	
	/**
	 * Calculates tpcOpId to be used for an operation. In this implementation
	 * it is a long variable that increases by one for each 2PC operation. 
	 * 
	 * @return 
	 */
	private String getNextTpcOpId() {
		tpcOpId++;
		return tpcOpId.toString();		
	}
	
	/**
	 * Start registration server in a separate thread
	 */
	private class RegistrationRunaable implements Runnable {

	    public void run() {
	    	NetworkHandler handler = new TPCRegistrationHandler(numSlaves);
			regServer.addHandler(handler);
			try{
				regServer.connect();
				regServer.run();
			} catch (IOException e){
				
			}
			
	    }
	}
	
	public void run() {
		AutoGrader.agTPCMasterStarted();
		System.out.println("Starting Registration Server");
		
		(new Thread(new RegistrationRunaable())).start();	
		
		AutoGrader.agTPCMasterFinished();
	}
	
	/**
	 * Converts Strings to 64-bit longs
	 * Borrowed from http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
	 * Adapted from String.hashCode()
	 * @param string String to hash to 64-bit
	 * @return
	 */
	private long hashTo64bit(String string) {
		// Take a large prime
		long h = 1125899906842597L; 
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31*h + string.charAt(i);
		}
		return h;
	}
	
	/**
	 * Compares two longs as if they were unsigned (Java doesn't have unsigned data types except for char)
	 * Borrowed from http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
	 * @param n1 First long
	 * @param n2 Second long
	 * @return is unsigned n1 less than unsigned n2
	 */
	private boolean isLessThanUnsigned(long n1, long n2) {
		return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
	}
	
	private boolean isLessThanEqualUnsigned(long n1, long n2) {
		return isLessThanUnsigned(n1, n2) || n1 == n2;
	}	

	/**
	 * Find first/primary replica location
	 * @param key
	 * @return
	 */
	private SlaveInfo findFirstReplica(String key) {
		// 64-bit hash of the key
		long hashedKey = hashTo64bit(key.toString());
		for (int i =0; i < slaveInfoList.size(); i++){
			if (isLessThanUnsigned (hashedKey, slaveInfoList.get(i).getSlaveID())){
				//Corner Case, if it is less than the first server and more than the last server then it belongs here
				if(i == 0){
					if(!isLessThanEqualUnsigned (hashedKey, slaveInfoList.get(slaveInfoList.size()-1).getSlaveID())){
						return slaveInfoList.get(i);
					}
				} else if(!isLessThanEqualUnsigned (hashedKey, slaveInfoList.get(i-1).getSlaveID()))
					return slaveInfoList.get(i);
			}
		}
		
		//If We're still here this must mean the hash is more than all of the slave Infos
		return slaveInfoList.get(slaveInfoList.size()-1);
	}
	
	/**
	 * Find the successor of firstReplica to put the second replica
	 * @param firstReplica
	 * @return
	 */
	private SlaveInfo findSuccessor(SlaveInfo firstReplica) {
		int index = slaveInfoList.indexOf(firstReplica);
		if(index == slaveInfoList.size()-1){
			return slaveInfoList.get(0);
		} else {
			return slaveInfoList.get(index+1);
		}
		
	}
	
	/**
	 * Synchronized method to perform 2PC operations one after another
	 * You will need to remove the synchronized declaration if you wish to attempt the extra credit
	 * 
	 * @param msg
	 * @param isPutReq
	 * @throws KVException
	 */
	public synchronized void performTPCOperation(KVMessage msg, boolean isPutReq) throws KVException {
		AutoGrader.agPerformTPCOperationStarted(isPutReq);
		//Get the Servers
		SlaveInfo firstSlaveServer = findFirstReplica(msg.getKey());
		SlaveInfo secondSlaveServer = findSuccessor(firstSlaveServer);
		
		System.out.println("Phase 1 Started");
		//Phase 1 -----------------------------------------------------------
		KVMessage tpcOperation;
		if(isPutReq){
			tpcOperation = new KVMessage( "putreq");
			tpcOperation.setValue(msg.getValue());			
		} else {
			tpcOperation = new KVMessage( "delreq");
		}
		
		String opId = getNextTpcOpId();
		tpcOperation.setKey(msg.getKey());
		tpcOperation.setTpcOpId(opId);
		
		//Send To Servers
		Socket firstSock = firstSlaveServer.connectHost();
		Socket secondSock = secondSlaveServer.connectHost();
		
		
		
		
		KVMessage firstResp;
		KVMessage secondResp;
		
		///Send to Server 1
		try {
			tpcOperation.sendMessage(firstSock);
			firstResp = new KVMessage(firstSock.getInputStream());
			System.out.println("Received: " + firstResp.toXML());
		} catch (IOException e) {
			firstSlaveServer.closeHost();
			throw new KVException(new KVMessage("resp", "Network Error: Could not receive data"));
		} finally {
			AutoGrader.agPerformTPCOperationFinished(isPutReq);
		}
		
		///Send to Server 2
		try {
			tpcOperation.sendMessage(secondSock);
			secondResp = new KVMessage(secondSock.getInputStream());
			System.out.println("Received: " + secondResp.toXML());
		} catch (IOException e) {
			KVMessage tpcAbort = new KVMessage("abort");
			tpcAbort.setTpcOpId(opId);
			tpcAbort.sendMessage(firstSock);
			
			firstSlaveServer.closeHost();
			secondSlaveServer.closeHost();
			throw new KVException(new KVMessage("resp", "Network Error: Could not receive data"));
		} finally {
			AutoGrader.agPerformTPCOperationFinished(isPutReq);
		}
		
		//If abort is replied
		KVMessage tpcReply;
		if (firstResp.getMsgType().equals("ready") && secondResp.getMsgType().equals("ready")){
			tpcReply = new KVMessage("commit");
		} else {
			tpcReply = new KVMessage("abort");
		}
		
		tpcReply.setTpcOpId(opId);
		
		//firstSlaveServer.closeHost();
		//secondSlaveServer.closeHost();
		
		System.out.println("Phase 2 Started");
		//Phase 2 ------------------------------------------------------------------
		
		firstSock = firstSlaveServer.connectHost();	
		//Server1
		while(true){
			tpcReply.sendMessage(firstSock);
			try {					
				firstResp = new KVMessage(firstSock.getInputStream());
				if(firstResp.getMsgType().equals("ack")){
					System.out.println("Received: " + firstResp.toXML());
					break;
			}
			} catch (IOException e) {
				int i = TIMEOUT_MILLISECONDS;
				while(i-- != 0){
					//Wait
				}
				continue;
			}				
		}
		//Server2
		secondSock = secondSlaveServer.connectHost();
		while(true){
			tpcReply.sendMessage(secondSock);
			try {
				secondResp = new KVMessage(secondSock.getInputStream());
				if(secondResp.getMsgType().equals("ack")){
					System.out.println("Received: " + secondResp.toXML());
					break;
				}
			} catch (IOException e) {
				int i = TIMEOUT_MILLISECONDS;
				while(i-- != 0){
					//Wait
				}
				continue;
			}				
		}
		firstSlaveServer.closeHost();
		secondSlaveServer.closeHost();
		
		AutoGrader.agPerformTPCOperationFinished(isPutReq);
		System.out.println("Phase 2 Finish");
		return;
	}

	/**
	 * Perform GET operation in the following manner:
	 * - Try to GET from first/primary replica
	 * - If primary succeeded, return Value
	 * - If primary failed, try to GET from the other replica
	 * - If secondary succeeded, return Value
	 * - If secondary failed, return KVExceptions from both replicas
	 * 
	 * @param msg Message containing Key to get
	 * @return Value corresponding to the Key
	 * @throws KVException
	 */
	public String handleGet(KVMessage msg) throws KVException {
		AutoGrader.aghandleGetStarted();
		
		String toReturn = null;

		SlaveInfo firstSlaveServer = findFirstReplica(msg.getKey());
		Socket firstSock = firstSlaveServer.connectHost();
		
		KVMessage resp = null;
		//Try First Server
		try{
			msg.sendMessage(firstSock);
			resp = new KVMessage(firstSock.getInputStream());
			if(resp.getValue() != null){
				toReturn = resp.getValue();
			}
		} catch (KVException | IOException e){
					
		}	
		firstSlaveServer.closeHost();
		
		//Try Second Server if still null
		if(toReturn == null){
			SlaveInfo secondSlaveServer = findFirstReplica(msg.getKey());
			Socket secondSock = secondSlaveServer.connectHost();
			msg.sendMessage(secondSock);
			
			try {
				resp = new KVMessage(secondSock.getInputStream());
				if(resp.getValue() != null){
					toReturn = resp.getValue();
				}
			} catch (IOException e1) {
				
			}	
			secondSlaveServer.closeHost();
		}
		
		AutoGrader.aghandleGetFinished();
		return toReturn;
	}
}
