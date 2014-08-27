/**
 * 
 * @creatTime 下午2:33:34
 * @author XuYi
 */
package org.eddy.tests;

import java.util.Random;

import org.eddy.datasource.HbaseConnection;
import org.eddy.datasource.HbaseDataSource;

/**
 * @author XuYi
 *
 */
public class DataSourceScanTest {

	public static void main(String[] args) throws InterruptedException {
		for (int i = 0; i < 50; i++) {
//			new Thread(new Runnable() {
//				
//				@Override
//				public void run() {
//					try {
//						Thread.sleep(new Random().nextInt(2000));
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					print();
//					
//				}
//			}).start();
//		}
			print();
		}
		Thread.sleep(1000000);
	}
	
	private static void print() {
		long begin = System.currentTimeMillis();
		HbaseConnection connection = HbaseDataSource.getInstance().getConnection("manageLog");
//		connection.scanString("1404696196527_606681627157035", "1404696196545_606681645051904", null, null, null);
		long end = System.currentTimeMillis();
		System.out.println(end - begin);
	}
}
