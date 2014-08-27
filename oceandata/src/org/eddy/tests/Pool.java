/**
 * 
 * @creatTime 下午1:57:57
 * @author XuYi
 */
package org.eddy.tests;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author XuYi
 *
 */
public class Pool {

	private static HTablePool pool;
	
	/**
	 * 
	 * @param args
	 * @creatTime 下午1:57:57
	 * @author XuYi
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master.hadoop,slave1.hadoop,slave2.hadoop");
		pool = new HTablePool(config, 10);
//		HTable table = (HTable) pool.getTable(Bytes.toBytes("manageLog"));
//		execute(table);
//		pool.putTable(table);
//		HTable table2 = (HTable) pool.getTable(Bytes.toBytes("manageLog"));
//		execute(table2);
//		pool.putTable(table2);
		for (int i = 0; i < 30; i++) {
			new Thread(new TestThread()).start();
		}
		
	}
	
	private static void execute(HTable table) throws IOException {
		table.setScannerCaching(5);
		long begin = System.currentTimeMillis();
		Scan scan = new Scan(Bytes.toBytes("1404696196527_606681627157035"));
		scan.setCacheBlocks(true);
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("message"));
		scan.setStopRow(Bytes.toBytes("1404696196536_606681636044792"));
		ResultScanner rs = table.getScanner(scan);
		Result res = null;
		try {
			while ((res = rs.next()) != null) {
				String rowid = Bytes.toString(res.getRow());
				String value = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("message")));
//				System.out.println(rowid + "     " + value);
			}
		} finally {
			rs.close();
		}
		long end = System.currentTimeMillis();
//		System.out.println(end - begin);
	}
	
	static class TestThread implements Runnable {
		
		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			while(true) {
				HTable table = (HTable) pool.getTable(Bytes.toBytes("manageLog"));
				try {
					execute(table);
					pool.putTable(table);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					try {
						Thread.sleep(new Random().nextInt(1000));
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
			
		}
		
	}

}
