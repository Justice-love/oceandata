/**
 * 
 * @creatTime 下午3:29:11
 * @author XuYi
 */
package org.eddy.tests;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author XuYi
 *
 */
public class GetTest {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master.hadoop,slave1.hadoop,slave2.hadoop");
		HTable table = new HTable(conf, Bytes.toBytes("test2"));
		Get get = new Get(Bytes.toBytes("1"));
		get.setCacheBlocks(true);
//		get.addColumn(Bytes.toBytes("today"), Bytes.toBytes("day"));
//		get.addColumn(Bytes.toBytes("today"), Bytes.toBytes("nextday"));
//		get.addFamily(Bytes.toBytes("f1"));
//		get.addFamily(Bytes.toBytes("f2"));
		Result result = table.get(get);
		System.out.println(result);
	}
}
