/**
 * 
 * @creatTime 下午2:06:25
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
public class TableFactoryTest implements HTableInterfaceFactory{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.client.HTableInterfaceFactory#createHTableInterface(org.apache.hadoop.conf.Configuration, byte[])
	 */
	@Override
	public HTableInterface createHTableInterface(Configuration conf, byte[] tableName) {
		try {
			HTable table =  new HTable(conf, tableName);
			System.out.println("创建table");
			return table;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.client.HTableInterfaceFactory#releaseHTableInterface(org.apache.hadoop.hbase.client.HTableInterface)
	 */
	@Override
	public void releaseHTableInterface(HTableInterface table) {
		if (null != table) {
			try {
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
