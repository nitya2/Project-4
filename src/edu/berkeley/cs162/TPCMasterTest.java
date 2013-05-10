package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TPCMasterTest {
	TPCMaster testMaster = null;
	Thread slaveOneThread = null;
	Thread slaveTwoThread = null;
	SlaveServer slave1 = null;;
	SlaveServer slave2 = null;
	@Before
	public void setUp() throws Exception {
		//testMaster = new TPCMaster(10);
		//testMaster.run();
		
		long slaveID1 = 0L;
		slave1 = new SlaveServer("localhost", 9090, 8080, slaveID1+ "@localhost:5151");
		long slaveID2 = 4611686018427387903L;
		slave2 = new SlaveServer("localhost", 9090, 8080, slaveID2+ "@localhost:5252");
			
		slaveOneThread = new Thread(slave1);
		slaveTwoThread = new Thread(slave2);
		
		
		slaveOneThread.start();
		slaveTwoThread.start();
	}

	@After
	public void tearDown() throws Exception {
		slaveOneThread.stop();
		slaveTwoThread.stop();
		slave1 = null;;
		slave2 = null;
		testMaster = null;
	}

	@Test
	public void testRegistration() {
		assertTrue(slave1.receivedRegistrationACK);
		assertTrue(slave2.receivedRegistrationACK);
	}
	
	
	
	
	
	
	public class SlaveServer implements Runnable{
		ServerSocket sock;
		boolean receivedRegistrationACK = false;
		
		boolean phase1Break = false;
		boolean phase2Break = false;
		
		public SlaveServer(String serverHostname, int serverPort,int port, String namePort){
			try{
				Socket regServer = new Socket(serverHostname, serverPort);
				KVMessage registration = new KVMessage("register");
				registration.setMessage(namePort);
				registration.sendMessage(regServer);
				
				KVMessage resp = new KVMessage(regServer.getInputStream());
				if(resp.getMessage().equals("Successfully registered " + namePort)){
					receivedRegistrationACK = true;
				}
				sock = new ServerSocket(port);
			} catch (Exception e) {
				
			}
		}
		@Override
		public void run() {
			while (true) {
				try{
					Socket clientSocket = this.sock.accept();
					
					if(phase2Break){
						
					} else {
					KVMessage req = new KVMessage(clientSocket.getInputStream());
						if(req.getMessage().equals("getreq")){
							KVMessage resp = new KVMessage("resp");
							resp.setValue("success");
							resp.sendMessage(clientSocket);
						}
						
						if(phase1Break){
							KVMessage resp = new KVMessage("abort");
							resp.setTpcOpId(req.getTpcOpId());
							resp.sendMessage(clientSocket);
						} else {
							KVMessage resp = new KVMessage("ready");
							resp.setTpcOpId(req.getTpcOpId());
							resp.sendMessage(clientSocket);
						}
						
						if(!phase2Break){
							KVMessage resp = new KVMessage("ack");
							resp.setTpcOpId(req.getTpcOpId());
							resp.sendMessage(clientSocket);
							clientSocket.close();
						} else {
							clientSocket.close();
						}
					}
				} catch (Exception e){
					
				}
			}			
		}		
	}

}
