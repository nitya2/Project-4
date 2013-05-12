package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import edu.berkeley.cs162.*;

public class TPCLogTest {
	
	static SocketServer server = null;
//	static TPCMaster master = null;
//	
//	@Before
//	public void setUp(){
//		master = new TPCMaster(6);
//		master.run();
//		
//		try {
//			server = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 8080);
//		} catch (UnknownHostException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		NetworkHandler handler = new KVClientHandler(master);
//		server.addHandler(handler);
//		try {
//			server.connect();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println("connected to server");
//		
//		Thread serverThread = new Thread(new Runnable(){
//			@Override 
//			public void run(){
//				try{
//					System.out.println("Starting server thread");
//					server.run();
//				}catch(IOException e){
//					e.printStackTrace();
//				}
//			}
//		});
//		serverThread.start();
//		
//	}

	@Test
	public void constructor(){
		long slaveID = -1;
		String logPath = null;
		TPCLog tpcLog = null;	
		KVServer keyServer = new KVServer(100, 10);
		try {
			server = new SocketServer(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logPath = slaveID + "@" + server.getHostname();
		//System.out.println(server.getHostname());
		
		tpcLog = new TPCLog(logPath, keyServer);
		
		assertNotNull("tpcLog is not null!", tpcLog);
		assertNull(tpcLog.getEntries());
	}
	
	@Test
	public void addEntry(){
		//KVServer keyserver = null;
		//Socket client = null;
		
		long slaveID = -1;
		String logPath = null;
		TPCLog tpcLog = null;	
		KVServer keyServer = new KVServer(100, 10);
		try {
			server = new SocketServer(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logPath = slaveID + "@" + server.getHostname();
		
		KVMessage m = null;
		try {
			m = new KVMessage("putreq");
			m.setKey("seven");
			m.setValue("7");
		} catch (KVException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		tpcLog = new TPCLog(logPath, keyServer);
		tpcLog.appendAndFlush(m);
		//tpcLog.loadFromDisk();
		//System.out.println(tpcLog.getEntries());
		assertFalse(tpcLog.empty());
		
		ArrayList<KVMessage> copy = tpcLog.getEntries();
		
		tpcLog.loadFromDisk();
		System.out.println(tpcLog.getEntries());
		System.out.println(copy);
		assertNotSame(tpcLog.getEntries(), copy);
		assertEquals(tpcLog.getEntries().get(0).getMsgType(), copy.get(0).getMsgType());
	}
	
	@Ignore
	public void rebuild(){
		long slaveID = -1;
		String logPath = null;
		TPCLog tpcLog = null;	
		KVServer keyServer = new KVServer(100, 10);
		try {
			server = new SocketServer(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logPath = slaveID + "@" + server.getHostname();
		//System.out.println(server.getHostname());
		
		tpcLog = new TPCLog(logPath, keyServer);
		
		try {
			tpcLog.rebuildKeyServer();
		} catch (KVException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//assertNotNull(k)
	}
	
//	@Test
//	public void test() {
//		fail("Not yet implemented");
//	}

}
