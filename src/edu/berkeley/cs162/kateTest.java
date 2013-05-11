package edu.berkeley.cs162;

import static org.junit.Assert.*;

import org.junit.Test;

public class kateTest {

	@Test
	public void test() {
		TPCMaster master = new TPCMaster(5);
		assertNotNull(master.makeSlaveInfo("000@Kate:8010"));
		assertNotNull(master.makeSlaveInfo("18924093@Verizion:80"));
		assertNotNull(master.makeSlaveInfo("0002232@Channing:6464"));
		
		assertNull(master.makeSlaveInfo("10000000000000000000@Channing:3939"));
		assertNull(master.makeSlaveInfo("1010239023@Channing:343:3433"));
		assertNull(master.makeSlaveInfo("33:Kate@4455"));

	}

}
