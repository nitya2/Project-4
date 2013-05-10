/**
 * Slave Server component of a KeyValue store
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

import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * This class defines the slave key value servers. Each individual KVServer 
 * would be a fully functioning Key-Value server. For Project 3, you would 
 * implement this class. For Project 4, you will have a Master Key-Value server 
 * and multiple of these slave Key-Value servers, each of them catering to a 
 * different part of the key namespace.
 *
 */
public class KVServer implements KeyValueInterface {
	private KVStore dataStore = null;
	private KVCache dataCache = null;
	
	private static final int MAX_KEY_SIZE = 256;
	private static final int MAX_VAL_SIZE = 256 * 1024;
	
	/**
	 * @param numSets number of sets in the data Cache.
	 */
	public KVServer(int numSets, int maxElemsPerSet) {
		dataStore = new KVStore();
		dataCache = new KVCache(numSets, maxElemsPerSet);
		
		AutoGrader.registerKVServer(dataStore, dataCache);
	}
	
	public void put(String key, String value) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerPutStarted(key, value);
		
		// TODO: implement me
		if(key.length() > KVServer.MAX_KEY_SIZE){
			KVMessage msgOverKey = new KVMessage("resp");
			msgOverKey.setMessage("Oversized key");
			
			throw new KVException(msgOverKey);
		}
		
		if(value.length() > KVServer.MAX_VAL_SIZE){
			KVMessage msgOverValue = new KVMessage("resp");
			msgOverValue.setMessage("Oversized value");
			throw new KVException(msgOverValue);
		}
		
		
		try{
			dataStore.put(key, value);
		} catch (KVException e){
			// Must be called before return or abnormal exit
			AutoGrader.agKVServerPutFinished(key, value);
			throw e;
		}
		
		WriteLock lock = dataCache.getWriteLock(key);
		lock.lock();
		dataCache.put(key,value);
		lock.unlock();
		// Must be called before return or abnormal exit
		AutoGrader.agKVServerPutFinished(key, value);
	}
	
	public String get (String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerGetStarted(key);
		
		// TODO: implement me
		//If KV pair is in the cache
		WriteLock lock = dataCache.getWriteLock(key);
		lock.lock();
		
		String toReturn = dataCache.get(key);
		if(toReturn != null){
			lock.unlock();
			AutoGrader.agKVServerGetFinished(key);
			return toReturn;
		}
		
		try{
			toReturn = dataStore.get(key);
		} catch (KVException e){
			lock.unlock();
			AutoGrader.agKVServerGetFinished(key);
			KVMessage msgDoesNotExist = new KVMessage("resp");
			msgDoesNotExist.setMessage("Does not exist");
			throw new KVException(msgDoesNotExist);
		}
		
		//If we found something in the dataStore, put it in the cache
		dataCache.put(key, toReturn);
		lock.unlock();
		// Must be called before return or abnormal exit
		AutoGrader.agKVServerGetFinished(key);
		return toReturn;
	}
	
	public void del (String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerDelStarted(key);

		// TODO: implement me
		try{
			dataStore.get(key);
			dataStore.del(key);
		} catch (KVException e){
			AutoGrader.agKVServerDelFinished(key);
			KVMessage msgDoesNotExist = new KVMessage("resp");
			msgDoesNotExist.setMessage("Does not exist");
			throw new KVException(msgDoesNotExist);
		}
		
		WriteLock lock = dataCache.getWriteLock(key);
		lock.lock();
		dataCache.del(key);
		lock.unlock();
		// Must be called before return or abnormal exit
		AutoGrader.agKVServerDelFinished(key);
	}
	
	public KVStore getDataStore(String password){
		if(password.equals("givemedatastore"))
			return dataStore;
		else
			return null;
	}
	
	public KVCache getDataCache(String password){
		if(password.equals("givemedatacache"))
			return dataCache;
		else
			return null;
	}
	public boolean hasKey (String key) throws KVException {
		// TODO: optional implement me
		
		return false;
	}
}
