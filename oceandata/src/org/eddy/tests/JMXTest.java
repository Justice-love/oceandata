/**
 * 
 * @creatTime 下午3:37:52
 * @author XuYi
 */
package org.eddy.tests;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * @author XuYi
 * 
 */
public class JMXTest {
	public static void main(String[] args) throws Exception {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

		ObjectName name = new ObjectName("org.eddy.tests:type=Echo");

		Echo mbean = new Echo();

		mbs.registerMBean(mbean, name);

		// mbs.invoke(name, "print", new Object[] { "haitao.tu"}, new String[]
		// {"java.lang.String"});
		// mbs.invoke(name, "getStr", new Object[] { "haitao.tu"}, new String[]
		// {"java.lang.String"});
		// mbs.getAttribute(name, "str");

		Thread.sleep(Long.MAX_VALUE);
	}
}
