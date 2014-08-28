/**
 * 
 * @creatTime 下午2:45:25
 * @author XuYi
 */
package org.eddy.exsample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.eddy.mine.DoubleColumnInterpreter;

/**
 * hbase分组函数学习
 * @author XuYi
 * 
 */
public class CoprocessorTest {

	static Configuration conf = HBaseConfiguration.create();
	static {
		conf.set("hbase.zookeeper.quorum", "master.hadoop,slave1.hadoop,slave2.hadoop");
	}
	public static void main(String[] args) throws Exception {
//		String coprocessClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
//		
//		HBaseAdmin admin = new HBaseAdmin(conf);
//		admin.disableTable("xyz");
//		HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes("xyz"));
//		htd.addCoprocessor(coprocessClassName);
//		admin.modifyTable(Bytes.toBytes("xyz"), htd);
//		admin.enableTable("xyz");
//		admin.close();
		sum();
		
	}
	
	public static void count() {
		long begin = System.currentTimeMillis();
		AggregationClient ac = new AggregationClient(conf);
		Scan scan = new Scan();
//		scan.setStartRow(Bytes.toBytes("3"));
//		scan.addColumn(Bytes.toBytes("fal"), Bytes.toBytes("val"));
		scan.addFamily(Bytes.toBytes("fal"));
		scan.setFilter(new FirstKeyOnlyFilter());
		long rowCount = 0;
		try {
			rowCount = ac.rowCount(Bytes.toBytes("test3"), new LongColumnInterpreter(), scan);
//			rowCount = ac.max(Bytes.toBytes("test"), new LongColumnInterpreter(), scan);
			System.out.println(rowCount);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println(end - begin);
	}
	
	public static void sum() {
		long begin = System.currentTimeMillis();
		AggregationClient ac = new AggregationClient(conf);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("detail"), Bytes.toBytes("value2"));
		Double sum = 0.0;
		try {
			sum = ac.max(Bytes.toBytes("xyz"), new DoubleColumnInterpreter(), scan);
//			ac.sum(Bytes.toBytes("xyz"), new DoubleColumnInterpreter(), scan);
//			ac.avg(Bytes.toBytes("xyz"), new DoubleColumnInterpreter(), scan);
//			ac.min(Bytes.toBytes("xyz"), new DoubleColumnInterpreter(), scan);
			System.out.println(sum);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println(end - begin);
	}
}
