/**
 * A simple thread pool implementation
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

import java.util.LinkedList;

public class ThreadPool {
	/**
	 * Set of threads in the threadpool
	 */
	protected Thread threads[] = null;
	//This is basically a queue, add to the end and remove from the front
	LinkedList<Runnable> jobsList;

	/**
	 * Initialize the number of threads required in the threadpool. 
	 * 
	 * @param size  How many threads in the thread pool.
	 */
	public ThreadPool(int size)
	{      
	    threads = new Thread[size];
	    jobsList = new LinkedList<Runnable>();
	    //start a new worker thread for each thread in threads
	    for (int i=0; i < size ; i++){
	    	threads[i] = new WorkerThread(this);
	    	//JVM will call run when start() is called
	    	threads[i].start();
	    }
	}

	/**
	 * Add a job to the queue of tasks that has to be executed. As soon as a thread is available, 
	 * it will retrieve tasks from this queue and start processing.
	 * @param r job that has to be executed asynchronously
	 * @throws InterruptedException 
	 */
	
	//KATE: ADDED SYNCHRONIZED keyword. Piazza seems to indicate this is fine. 
	//Basically, wait and notify need to be synched on the same object this ensures that.
	public synchronized void addToQueue(Runnable r) throws InterruptedException
	{
	      
	    	  jobsList.addLast(r);
	    	//Gotta wake up sleeping/waiting threads at this point
	    	  notify();
	      

	      

	}
	
	/** 
	 * Block until a job is available in the queue and retrieve the job
	 * @return A runnable task that has to be executed
	 * @throws InterruptedException 
	 */
	public synchronized Runnable getJob() throws InterruptedException {
	  
		   while (jobsList.isEmpty()){
	   
	    	
	    		wait();
	    	
	    }
	  
	    return jobsList.removeFirst();
	}
}

/**
 * The worker threads that make up the thread pool.
 */
class WorkerThread extends Thread {
	/**
	 * The constructor.
	 * 
	 * @param o the thread pool 
	 */
	ThreadPool parentThreadPool;
	boolean isRunning;
	WorkerThread(ThreadPool o)
	{
	     parentThreadPool =o;
	     isRunning = true;
	}

	/**
	 * Scan for and execute tasks.
	 */
	public void run()
	{
	      while(isRunning){
	    	try {
				parentThreadPool.getJob().run();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	      }
	}
}
