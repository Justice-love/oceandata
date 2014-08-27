/**
 * 
 * @creatTime 下午3:19:22
 * @author XuYi
 */
package org.eddy.tests;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.eddy.pool.TableFactory;

/**
 * @author XuYi
 *
 */
public class PutTest {
	static Configuration conf = HBaseConfiguration.create();
	static {
		conf.set("hbase.zookeeper.quorum", "master.hadoop,slave1.hadoop,slave2.hadoop");
	}
	static HTablePool pool = new HTablePool(conf, 200, new TableFactory2());
	static AtomicLong atomic = new AtomicLong(2800000);
	
	public static void main(String[] args) throws IOException {
		
		
//		HTable table = new HTable(conf, "test2");
//		table.setAutoFlush(false);
//		Put put = new Put(Bytes.toBytes("1"));
////		System.out.println(put.has(Bytes.toBytes("today"), Bytes.toBytes("day")));
//		put.add(Bytes.toBytes("f1"), Bytes.toBytes("d1"), Bytes.toBytes("周一"));
//		put.add(Bytes.toBytes("f2"), Bytes.toBytes("d2"), Bytes.toBytes("周一"));
////		System.out.println(put.has(Bytes.toBytes("today"), Bytes.toBytes("day")));
//		table.put(put);
//		table.flushCommits();
		for (int i = 0; i < 200; i++) {
			new MyThread().start();
		}
	}
	
	static class MyThread extends Thread {
		/* (non-Javadoc)
		 * @see java.lang.Thread#run()
		 */
		@Override
		public void run() {
			while (true) {
				HTableInterface table = null;
				long i = atomic.getAndIncrement();
				if (i > 7000000) {
					break;
				}
				try {
					table = pool.getTable(Bytes.toBytes("xyz"));
					Put put = new Put(Bytes.toBytes(i + ""));
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("val"), Bytes.toBytes(i + ""));
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("val2"), Bytes.toBytes("v"));
					table.put(put);
					System.out.println(i);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					try {
						table.flushCommits();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					pool.putTable(table);
				}
			}
		}
	}
}
