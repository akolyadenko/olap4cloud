package org.olap4cloud.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class CubeScanFilter implements Filter {

	CubeScan scan;
	
	public CubeScanFilter(CubeScan scan) {
		this.scan = scan;
	}
	
	@Override
	public boolean filterAllRemaining() {
		return false;
	}

	@Override
	public ReturnCode filterKeyValue(KeyValue keyVal) {
		byte buf[] = keyVal.getBuffer();
		int keyOffset = keyVal.getKeyOffset();
		for(CubeScanCondition condition: scan.getConditions()) {
			long dimValue = Bytes.toLong(buf, keyOffset + 8 * (condition.getDimensionNumber() - 1));
			if(Arrays.binarySearch(condition.getValues(), dimValue) < 0)
				return ReturnCode.SKIP;
		}
		return ReturnCode.INCLUDE;
	}

	@Override
	public boolean filterRow() {
		return false;
	}

	@Override
	public boolean filterRowKey(byte[] arg0, int arg1, int arg2) {
		return false;
	}

	@Override
	public void reset() {
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	}

}
