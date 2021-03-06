/**
 * Log for Two-Phase Commit
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
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
 *  DISCLAIMED. IN NO EVENT SHALL PRASHANTH MOHAN BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;

public class TPCLog {

	// Path to log file
	private String logPath = null;
	// Reference to the KVServer of this slave. Populated by rebuildKeyServer()
	private KVServer kvServer = null;

	// Log entries
	private ArrayList<KVMessage> entries = null;
	private ArrayList<KVMessage> reEntry = null;
	
	/*  Keeps track of the interrupted 2PC operation.
	 There can be at most one, i.e., when the last 2PC operation before
	 crashing was in READY state.
	 Set in  rebuildKeyServer() during recovery */ 
	private KVMessage interruptedTpcOperation = null;
	
	/**
	 * 
	 * @param logPath 
	 * @param kvServer Reference to the KVServer of this slave. Populated by
	 * rebuildKeyServer() during start. 
	 */
	public TPCLog(String logPath, KVServer kvServer) {
		this.logPath = logPath;
		entries = null;
		this.kvServer = kvServer;
	}

	public ArrayList<KVMessage> getEntries() {
		return entries;
	}

	public boolean empty() {
		return (entries.size() == 0);
	}
	
	public void appendAndFlush(KVMessage entry) {
		// implement me
		if(entries == null){	//if entries is null, instantiate it
			entries = new ArrayList<KVMessage>(); 
		}
		
		entries.add(entry);	//append it to the end of the ArrayList entries
		this.flushToDisk();	//flush the message away using flushToDisk
	}

	/**
	 * Load log from persistent storage
	 */
	@SuppressWarnings("unchecked")
	public void loadFromDisk() {
		ObjectInputStream inputStream = null;
		
		try {
			inputStream = new ObjectInputStream(new FileInputStream(logPath));			
			entries = (ArrayList<KVMessage>) inputStream.readObject();
		} catch (Exception e) {
			
		} finally {
			// If log never existed, there are no entries
			if (entries == null) {
				entries = new ArrayList<KVMessage>();
			}

			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Writes log to persistent storage
	 */
	public void flushToDisk() {
		ObjectOutputStream outputStream = null;
		
		try {
			outputStream = new ObjectOutputStream(new FileOutputStream(logPath));
			outputStream.writeObject(entries);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (outputStream != null) {
					outputStream.flush();
					outputStream.close();
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Load log and rebuild by iterating over log entries
	 * Set interruptedTpcOperation, if there is one (i.e., SlaveServer crashed
	 * in the READY state)
	 * @throws KVException
	 */
	public void rebuildKeyServer() throws KVException {
		// implement me
		try{
			this.loadFromDisk(); 	//fills entries with KVMessage logs
		} catch (Exception e){
			e.printStackTrace();
		}
		if(empty()){	//if entries is empty, nothing to rebuild
			return;
		}
		
		reEntry = new ArrayList<KVMessage>();
		
		KVMessage next =  null;
		Iterator<KVMessage> iter = entries.iterator();	//iterate over entries ArrayList to get all KVMessages
		while(iter.hasNext()){				//send KVMessages to KVServer (rebuild)
			KVMessage mess = iter.next();
			if (mess.getMsgType().equals("putreq") || mess.getMsgType().equals("delreq")){
				if(!iter.hasNext()){		//if the last message was a put/del, it was interrupted
					interruptedTpcOperation = mess;		//set interruptedTpcOperation to be last KVMessage
					return;						
				}
				next = iter.next();
				if (next.getMsgType().equals("abort")){	//If aborted, don't add it or mess to reEntry
					continue;
				}
			}
			reEntry.add(mess);	
			if(next != null) {
				reEntry.add(next);
				next = null;			//make next null again
			}
		}
		
		//now reEntry should be full of KVMessages to send to KVServer
		Iterator<KVMessage> reIter = reEntry.iterator();
		while(reIter.hasNext()){
			KVMessage reMess = reIter.next();
			if(reMess.getMsgType().equals("getreq")){
				System.out.println("Getting: " + reMess.getKey());
				kvServer.get(reMess.getKey());
			}
			if(reMess.getMsgType().equals("putreq")){
				System.out.println("Putting: "  + reMess.getKey() + " " + reMess.getValue());
				kvServer.put(reMess.getKey(), reMess.getValue());
			}
			if(reMess.getMsgType().equals("delreq")){
				System.out.println("Deleting: "  + reMess.getKey());
				kvServer.del(reMess.getKey());
			}
		}

		reEntry.clear();	//empty reEntry so that it can be filled again
	}
	
	/**
	 * 
	 * @return Interrupted 2PC operation, if any 
	 */
	public KVMessage getInterruptedTpcOperation() { 
		KVMessage logEntry = interruptedTpcOperation; 
		interruptedTpcOperation = null; 
		return logEntry; 
	}
	
	/**
	 * 
	 * @return True if TPCLog contains an interrupted 2PC operation
	 */
	public boolean hasInterruptedTpcOperation() {
		return interruptedTpcOperation != null;
	}
}
