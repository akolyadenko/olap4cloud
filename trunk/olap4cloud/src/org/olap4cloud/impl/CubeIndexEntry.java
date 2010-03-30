package org.olap4cloud.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CubeIndexEntry implements Writable, Comparable<CubeIndexEntry> {
	private int length = 0;
	
	private byte data[];
	
	public CubeIndexEntry() {
		
	}
	
	public CubeIndexEntry(int length, byte data[]) {
		this.length = length;
		this.data = data;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		length = in.readInt();
		data = new byte[length];
		in.readFully(data);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(length);
		out.write(data, 0, length);
	}

	@Override
	public int compareTo(CubeIndexEntry o) {
		for(int i = 0; i < o.length && i < length; i ++) 
			if(data[i] != o.data[i])
				return data[i] - o.data[i];
		return length - o.length;
	}
	
	public boolean contain(CubeIndexEntry e) {
		if(e.length < length)
			return false;
		for(int i = 0; i < length; i ++)
			if(data[i] != e.data[i])
				return false;
		return true;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
}
