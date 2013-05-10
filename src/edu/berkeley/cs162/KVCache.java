/**
 * Implementation of a set-associative cache.
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

import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;


/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is dropped based on the eviction policy.
 */
public class KVCache implements KeyValueInterface {	
	private int numSets = 100;
	private int maxElemsPerSet = 10;
	
	private LinkedList<cacheEntry>[] cacheSet = null;
	private WriteLock[] arrayOfLocks = null;
	/**
	 * Creates a new LRU cache.
	 * @param cacheSize	the maximum number of entries that will be kept in this cache.
	 */
	public KVCache(int numSets, int maxElemsPerSet) {
		this.numSets = numSets;
		this.maxElemsPerSet = maxElemsPerSet;     
		// TODO: Implement Me!
		cacheSet = (LinkedList<cacheEntry>[]) new LinkedList[this.numSets];
		
		for(int j = 0; j < this.numSets; j++){
			cacheSet[j] = new LinkedList<cacheEntry>();
		}
		
		arrayOfLocks = new WriteLock[this.numSets];
		
		for(int i = 0; i < this.numSets; i++){
			ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
			arrayOfLocks[i] = rwl.writeLock();
		}
	}

	/**
	 * Retrieves an entry from the cache.
	 * Assumes the corresponding set has already been locked for writing.
	 * @param key the key whose associated value is to be returned.
	 * @return the value associated to this key, or null if no value with this key exists in the cache.
	 */
	public String get(String key) {
		// Must be called before anything else
		AutoGrader.agCacheGetStarted(key);
		AutoGrader.agCacheGetDelay();
        
		// TODO: Implement Me!
		int setLocation = getSetId(key);
		String toReturn = null;
		LinkedList<cacheEntry> kvSet = cacheSet[setLocation];
		
		cacheEntry entry = null;
		for(int i = 0; i < kvSet.size(); i++){
			entry = kvSet.get(i);
			if(entry.key.equals(key)){
				toReturn = entry.value;
				entry.useBit = true;
				break;
			}
		}
		
		// Must be called before returning
		AutoGrader.agCacheGetFinished(key);
		return toReturn;
	}

	/**
	 * Adds an entry to this cache.
	 * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
	 * If the cache is full, an entry is removed from the cache based on the eviction policy
	 * Assumes the corresponding set has already been locked for writing.
	 * @param key	the key with which the specified value is to be associated.
	 * @param value	a value to be associated with the specified key.
	 * @return true is something has been overwritten 
	 */
	public void put(String key, String value) {
		// Must be called before anything else
		AutoGrader.agCachePutStarted(key, value);
		AutoGrader.agCachePutDelay();

		// TODO: Implement Me!
		int setLocation = getSetId(key);
		
		//Find the set
		LinkedList<cacheEntry> kvSet = cacheSet[setLocation];

		//First we will search to see if the List Exists 
		cacheEntry entry = null;
		boolean found = false;
		
		for(int i = 0; i < kvSet.size(); i++){
			entry = kvSet.get(i);
			
			
			//Check if the key matches
			//Assuming "==" matches the string characters
			if(entry.key.equals(key)){
				entry.value = value;
				entry.useBit = false;
				found = true;
				break;
			}
		}
		
		//If we haven't found entry in the scan
		if(!found){
			boolean addComplete = false;
			
			/* ****************************************************
			 * Since we're moving around nodes in the LinkedList, 
			 * we need to account for that when we scan           */
			
			//Default Case.
			//When there is space available
			if(kvSet.size() < this.maxElemsPerSet){
				cacheEntry newEntry = new cacheEntry(key, value);
				kvSet.add(newEntry);
				addComplete = true;
			}
			
			entry = null;
			//Case when list is full
			while(!addComplete){
				//Always check the head (this is a changing element)
				entry = kvSet.getFirst();
				if(entry.useBit){
					entry.useBit = false;
					kvSet.removeFirst();
					kvSet.add(entry);
				} else {
					cacheEntry newEntry = new cacheEntry(key, value);
					//Remove head
					kvSet.removeFirst();
					//Add new entry
					kvSet.add(newEntry);
					addComplete = true;
				}
			}
		}
		// Must be called before returning
		AutoGrader.agCachePutFinished(key, value);
	}

	/**
	 * Removes an entry from this cache.
	 * Assumes the corresponding set has already been locked for writing.
	 * @param key	the key with which the specified value is to be associated.
	 */
	public void del (String key) {
		// Must be called before anything else
		AutoGrader.agCacheGetStarted(key);
		AutoGrader.agCacheDelDelay();
		
		// TODO: Implement Me!
		int setLocation = getSetId(key);
		
		//Find the set
		LinkedList<cacheEntry> kvSet = cacheSet[setLocation];
		//First we will search to see if the List Exists 
		cacheEntry entry = null;
		
		for(int i = 0; i < kvSet.size(); i++){
			entry = kvSet.get(i);
			if(entry.key.equals(key)){
				kvSet.remove(i);
				break;
			}
		}
		// Must be called before returning
		AutoGrader.agCacheDelFinished(key);
	}
	
	/**
	 * @param key
	 * @return	the write lock of the set that contains key.
	 */
	public WriteLock getWriteLock(String key) {
	    // TODO: Implement Me!
		int setLocation = getSetId(key);
		//Find the set
		LinkedList<cacheEntry> kvSet = cacheSet[setLocation];
		//First we will search to see if the List Exists 
		cacheEntry entry = null;
		
		return arrayOfLocks[setLocation];
		
		/*
		//If the key exists in the cache, return the lock
		for(int i = 0; i < kvSet.size(); i++){
			entry = kvSet.get(i);
			if(entry.key == key){
				return arrayOfLocks[setLocation];
			}
		}
		
	    return null;
	    
	    */
	}
	
	/**
	 * 
	 * @param key
	 * @return	set of the key
	 */
	private int getSetId(String key) {
		return Math.abs(key.hashCode()) % numSets;
	}
	
    public String toXML() throws Exception{

        XMLOutputFactory factory = XMLOutputFactory.newInstance();
     	XMLStreamWriter writer = factory.createXMLStreamWriter(new FileWriter("cachedump.xml"));
		writer.writeStartDocument();
		writer.writeStartElement("KVCache");
		int i = 0;
		while (i < cacheSet.length) {
			i++;
			LinkedList<cacheEntry> listentries = cacheSet[i];
			
			writer.writeStartElement("Set");
			writer.writeAttribute("Id", Integer.toString(i));
			Iterator<cacheEntry> entries = listentries.iterator();
			while (entries.hasNext()) {
				String refd = "false";
				cacheEntry curr = entries.next();
   				if (curr.useBit) {
					refd = "true";
				}
			writer.writeStartElement("CacheEntry");
			writer.writeAttribute("isReferenced", refd);
			writer.writeAttribute("isValid", "true");
			writer.writeStartElement("Key");
			writer.writeCharacters(curr.key);
			writer.writeEndElement();
			writer.writeStartElement("Value");
			writer.writeCharacters(curr.value);
			writer.writeEndElement();
			writer.writeEndElement();
			}
			writer.writeEndElement();
		}
		
		writer.writeEndElement();

     		writer.flush();
     		writer.close();
		FileInputStream cachedump = new FileInputStream("cachedump.xml");
		String toxml = new Scanner( cachedump ).useDelimiter("\\A").next();
		return toxml;
    }
    

    public void printList(String key){
    	int setLocation = getSetId(key);
		//Find the set
		LinkedList<cacheEntry> kvSet = cacheSet[setLocation];
		cacheEntry entry = null;
		for(int i = 0; i < kvSet.size(); i++){
			entry = kvSet.get(i);
			System.out.println(i+1 + ". " + entry.value + "     UseBit: " + entry.useBit);
		}
    }
    
	private class cacheEntry {
		String key;
		String value;
		boolean	useBit;
		
		public cacheEntry(String aKey, String aValue){
			this.key = aKey;
			this.value = aValue;
			this.useBit = false;
		}
	}
}

