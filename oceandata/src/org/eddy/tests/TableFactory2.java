/**
 * 
 * @creatTime 下午2:54:59
 * @author XuYi
 */
package org.eddy.tests;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;

/**
 * @author XuYi
 *
 */
public class TableFactory2 implements HTableInterfaceFactory {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.client.HTableInterfaceFactory#createHTableInterface(org.apache.hadoop.conf.Configuration, byte[])
	 */
	@Override
	public HTableInterface createHTableInterface(Configuration conf, byte[] tableName) {
		if (null == conf || 0 == tableName.length) {
			return null;
		}
		HTable table;
		try {
			table = new HTable(conf, tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		table.setAutoFlush(false);
		table.setScannerCaching(20);
		return table;
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.client.HTableInterfaceFactory#releaseHTableInterface(org.apache.hadoop.hbase.client.HTableInterface)
	 */
	@Override
	public void releaseHTableInterface(HTableInterface table) {
		if (null == table) {
			return;
		}
		try {
			table.flushCommits();
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
