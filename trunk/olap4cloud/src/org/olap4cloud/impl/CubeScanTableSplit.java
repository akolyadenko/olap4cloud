package org.olap4cloud.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class CubeScanTableSplit extends InputSplit implements Writable, Comparable<CubeScanTableSplit> {

	private byte[] tableName;
	private byte[] startRow;
	private byte[] endRow;
	private String regionLocation;

	public CubeScanTableSplit(byte[] tableName, byte[] startRow, byte[] endRow, final String location) {
		this.tableName = tableName;
		this.startRow = startRow;
		this.endRow = endRow;
		this.regionLocation = location;
	}

	public byte[] getStartRow() {
		return startRow;
	}

	public byte[] getEndRow() {
		return endRow;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] { regionLocation };
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tableName = Bytes.readByteArray(in);
		startRow = Bytes.readByteArray(in);
		endRow = Bytes.readByteArray(in);
		regionLocation = Bytes.toString(Bytes.readByteArray(in));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, tableName);
		Bytes.writeByteArray(out, startRow);
		Bytes.writeByteArray(out, endRow);
		Bytes.writeByteArray(out, Bytes.toBytes(regionLocation));
	}

	@Override
	public String toString() {
		return regionLocation + ":" + Bytes.toStringBinary(startRow) + "," + Bytes.toStringBinary(endRow);
	}

	@Override
	public int compareTo(CubeScanTableSplit split) {
		return Bytes.compareTo(getStartRow(), split.getStartRow());
	}

}
