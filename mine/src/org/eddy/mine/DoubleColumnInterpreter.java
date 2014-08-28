package org.eddy.mine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

public class DoubleColumnInterpreter implements ColumnInterpreter<Double, Double> {

	@Override
	public void write(DataOutput out) throws IOException {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
	}

	@Override
	public Double add(Double l1, Double l2) {
		if (l1 == null ^ l2 == null) {
			return l1 == null ? l2 : l1;
		} else if (l1 == null) {
			return null;
		}
		return l1 + l2;
	}

	@Override
	public Double getMaxValue() {
		return null;
	}

	@Override
	public Double getMinValue() {
		return null;
	}

	@Override
	public Double multiply(Double o1, Double o2) {
		if (o1 == null ^ o2 == null) {
			return o1 == null ? o2 : o1;
		} else if (o1 == null) {
			return null;
		}
		return o1 * o2;
	}

	@Override
	public Double increment(Double o) {
		return null;
	}

	@Override
	public Double castToReturnType(Double o) {
		return o.doubleValue();
	}

	@Override
	public int compare(Double l1, Double l2) {
		if (l1 == null ^ l2 == null) {
			return l1 == null ? -1 : 1; // either of one is null.
		} else if (l1 == null)
			return 0; // both are null
		return l1.compareTo(l2); // natural ordering.
	}

	@Override
	public double divideForAvg(Double o, Long l) {
		return (o == null || l == null) ? Double.NaN : (o.doubleValue() / l.doubleValue());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.hbase.coprocessor.ColumnInterpreter#getValue(byte[],
	 * byte[], org.apache.hadoop.hbase.KeyValue)
	 */
	@Override
	public Double getValue(byte[] arg0, byte[] arg1, KeyValue arg2) throws IOException {
		if (arg2 == null)
			return null;
		return Bytes.toDouble(arg2.getValue());
	}
}