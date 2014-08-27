/**
 * 
 * @creatTime 上午11:03:28
 * @author XuYi
 */
package org.eddy.tests;

import org.eddy.register.Register;

/**
 * @author XuYi
 *
 */
public class MineTest {

	public static void main(String[] args) {
		Register.getInstance().register(new Class<?>[]{EddyTest.class});
		EddyTest test = Register.getInstance().getRegister(EddyTest.class);
//		test.insert("2013", "7", "17", "4");
//		System.out.println(test.selectAll("0", "3"));
//		System.out.println(test.selectAll2("0", "5", "2013"));
//		System.out.println(test.selectAll3("0", "5", "2014"));
		System.out.println(test.getOne("one"));
	}
}
