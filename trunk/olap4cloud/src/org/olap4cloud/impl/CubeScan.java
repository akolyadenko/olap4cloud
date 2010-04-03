package org.olap4cloud.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

public class CubeScan {
	
	List<Pair<byte[], byte[]>> ranges = new ArrayList<Pair<byte[],byte[]>>();
	
	public CubeScan() {
		
	}

	public List<Pair<byte[], byte[]>> getRanges() {
		return ranges;
	}
	
	public Scan getHBaseScan() {
		return new Scan();
	}
}
