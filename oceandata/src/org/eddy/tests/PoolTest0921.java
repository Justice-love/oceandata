/**
 * 
 * @creatTime 上午11:25:22
 * @author XuYi
 */
package org.eddy.tests;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author XuYi
 *
 */
public class PoolTest0921 {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master.hadoop,slave1.hadoop,slave2.hadoop");
		HTablePool pool = new HTablePool(conf, 1, new TableFactory2());
		HTableInterface table = pool.getTable(Bytes.toBytes("test3"));
		
		Get get1 = new Get(Bytes.toBytes("1"));
		table.get(get1);
		System.out.println(table);
		
		table.close();
		
		HTableInterface table2 = pool.getTable(Bytes.toBytes("test3"));
		table.get(get1);
		System.out.println(table2);
		table2.close();
	}
}
