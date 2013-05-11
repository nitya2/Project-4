package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class kateTest {
	
	static SocketServer server = null;
	static TPCMaster master = null;
	@Before
	public void setUp(){
		master = new TPCMaster(6);
		master.run();
		
		try {
			server = new SocketServer(InetAddress.getLocalHost().getHostAddress(), 8080);
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		NetworkHandler handler = new KVClientHandler(master);
		server.addHandler(handler);
		try {
			server.connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("connected to server");
		
		Thread serverThread = new Thread(new Runnable(){
			@Override 
			public void run(){
				try{
					System.out.println("Starting server thread");
					server.run();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		});
		serverThread.start();
		
	}
	@Test
	public void test() {
		System.out.println("running slaveinfo parsing test...");
		TPCMaster master = new TPCMaster(5);
		assertNotNull(master.makeSlaveInfo("000@Kate:8010"));
		assertNotNull(master.makeSlaveInfo("18924093@Verizion:80"));
		assertNotNull(master.makeSlaveInfo("0002232@Channing:6464"));
		
		assertNull(master.makeSlaveInfo("10000000000000000000@Channing:3939"));
		assertNull(master.makeSlaveInfo("1010239023@Channing:343:3433"));
		assertNull(master.makeSlaveInfo("33:Kate@4455"));
		System.out.println("completed slaveinfo parsing test.");

	}
	@Test
	public void slaveRegistration(){
		Random r = new Random();
		
		//set up a bunch of random longs
		ArrayList longIds = new ArrayList();
		for (int i =0; i < 7; i++){
			longIds.add(r.nextLong());
		}
		System.out.println("running slave register test...");
		for (int i=0; i < 7 ;i++){
			try {
				KVMessage regMessage;
				Socket socket = new Socket("localhost",9090);
				String registration = longIds.get(i) + "@localhost:9090";
				regMessage = new KVMessage("register", registration);
				regMessage.sendMessage(socket);
				KVMessage result = new KVMessage(socket.getInputStream());
				System.out.println(result.getMessage());
			}catch(UnknownHostException e){
				System.err.println(e.getMessage());
			}catch (IOException e){
				System.err.println(e.getMessage());

			} catch (KVException e) {
				// TODO Auto-generated catch block
				System.err.println(e.getMsg().getMessage());
			}
		}
	}
}
