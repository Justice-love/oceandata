/**
 * 
 * @creatTime 下午3:30:10
 * @author XuYi
 */
package org.eddy.cache;

import org.eddy.datasource.HbaseConnection;
import org.eddy.datasource.HbaseConnection.HbaseMethod;

/**
 * @author XuYi
 * 
 */
public class CacheKey {

	private String row;
	private String[] familay;
	private String[] qualifier;
	private HbaseConnection.HbaseMethod scan;

	public CacheKey(String row, String[] familay, String[] qualifier, HbaseMethod scan) {
		super();
		this.row = row;
		this.familay = familay;
		this.qualifier = qualifier;
		this.scan = scan;
	}

	public String getRow() {
		return row;
	}

	public void setRow(String row) {
		this.row = row;
	}

	public String[] getFamilay() {
		return familay;
	}

	public void setFamilay(String[] familay) {
		this.familay = familay;
	}

	public String[] getQualifier() {
		return qualifier;
	}

	public void setQualifier(String[] qualifier) {
		this.qualifier = qualifier;
	}

	public HbaseConnection.HbaseMethod getScan() {
		return scan;
	}

	public void setScan(HbaseConnection.HbaseMethod scan) {
		this.scan = scan;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		// boolean flag = false;
		// CacheKey o = (CacheKey)obj;
		// if (o == this) {
		// flag = true;
		// } else if
		// ((o.getFamilay()==null?"":o.getFamilay()).equals((this.familay ==
		// null?"":this.familay)) &&
		// (o.getRow()==null?"":o.getRow()).equals((this.row ==
		// null?"":this.row)) &&
		// (o.getQualifier()==null?"":o.getQualifier()).equals((this.qualifier
		// == null?"":this.qualifier)) && o.getScan() == this.scan) {
		// flag = true;
		// } else {
		// flag = false;
		// }
		// return flag;
		CacheKey o = (CacheKey) obj;
		if (o.getScan() == this.getScan()) {
			return ("" + this.getRow() + this.getFamilay() + this.getQualifier()).equals("" + o.getRow() + o.getFamilay() + o.getQualifier());
		} else {
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "" + this.getScan() + this.getRow() + this.getFamilay() + this.getQualifier();
	}

	public static void main(String[] args) {
		// CacheKey key =
		// HbaseConnection.createCacheKey("1404696196527_606681627157035" + "|"
		// + "1404696196545_606681645051904", null, null, HbaseMethod.Scan);
		// CacheKey key2 =
		// HbaseConnection.createCacheKey("1404696196527_606681627157035" + "|"
		// + "1404696196545_606681645051904", null, null, HbaseMethod.Scan);
		System.out.println("" + HbaseMethod.Scan);
	}
}
