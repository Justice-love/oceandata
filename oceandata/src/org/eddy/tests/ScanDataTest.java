/**
 * 
 * @creatTime 下午1:18:36
 * @author XuYi
 */
package org.eddy.tests;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author XuYi
 *
 */
public class ScanDataTest {

	public static void main(String[] args) throws IOException {
		AtomicInteger i = new AtomicInteger(0);
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master.hadoop,slave1.hadoop,slave2.hadoop");
		HTable table = new HTable(conf, "message_log");
		table.setScannerCaching(5000);
		long begin = System.currentTimeMillis();
		Scan scan = new Scan();
		scan.setCacheBlocks(true);//1408411794515 1408411793215 1408411804515   1408411794515
//		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("1408411794515"));
//		scan.setFilter(filter);
		scan.addColumn(Bytes.toBytes("detail"), Bytes.toBytes("value"));
//		scan.setStartRow(Bytes.toBytes("1408411804515"));
//		scan.setStopRow(Bytes.toBytes("1408411806915"));
		ResultScanner rs = table.getScanner(scan);
		Result res = null;
		double max = 0.0;
		try {
			while ((res = rs.next()) != null) {
//				String rowid = Bytes.toString(res.getRow());
//				String value = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("message")));
//				System.out.println(res);
				double v = Double.parseDouble(Bytes.toString(res.getValue(Bytes.toBytes("detail"), Bytes.toBytes("value"))));
//				System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("detail"), Bytes.toBytes("value"))));
				if (v > max) {
					max = v;
				}
				i.getAndIncrement();
//				System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("d1"))));
			}
		} finally {
			rs.close();
		}
		long end = System.currentTimeMillis();
		System.out.println(end - begin);
		System.out.println(max);
		System.out.println(i.get());
	}
}
