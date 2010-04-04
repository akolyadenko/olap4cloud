package org.olap4cloud.client;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.log4j.Logger;
import org.olap4cloud.impl.CubeIndexEntry;
import org.olap4cloud.impl.CubeScan;
import org.olap4cloud.impl.CubeScanMR;
import org.olap4cloud.impl.OLAPEngineConstants;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;


public class OLAPEngine {
	
	static Logger logger = Logger.getLogger(OLAPEngine.class);
	
	HBaseConfiguration config = new HBaseConfiguration();
	
	public OLAPEngine() {
		
	}
	
	public CubeQueryResult executeQuery(CubeQuery query, CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		try {
			CubeScan scan = getCubeScan(query, cubeDescriptor);
			return CubeScanMR.scan(scan, cubeDescriptor);
		} catch(Exception e) {
			throw new OLAPEngineException(e);
		}
	}
	
	CubeScan getCubeScan(CubeQuery query, CubeDescriptor cubeDescriptor) throws Exception{
		String methodName  = "getCubeScan() ";
		CubeScan scan = new CubeScan();
		List<CubeIndexEntry> index = null;
		for(CubeQueryCondition condition: query.getConditions()) {
			String dimensionName = condition.getDimensionName();
			if(logger.isDebugEnabled()) logger.debug(methodName + "process dimension " + dimensionName);
			int dimensionNumber = getDimensionNumber(dimensionName, cubeDescriptor);
			if(logger.isDebugEnabled()) logger.debug(methodName + "dimensionNumber = " + dimensionNumber);
			List<CubeIndexEntry> dimIndex = new ArrayList<CubeIndexEntry>();
			for(long dimVal: condition.dimensionValues) {
				List<CubeIndexEntry> dimValIndex = getIndexForDimensionValue(dimensionNumber, dimVal, cubeDescriptor);
				dimIndex = mergeIndexes(dimIndex, dimValIndex);
			}
			if(index == null)
				index = dimIndex;
			else
				index = joinIndexes(index, dimIndex);
		}
		if(logger.isDebugEnabled()) logger.debug(methodName + "final index size: " + index.size());
		for(CubeIndexEntry indexEntry: index) {
			byte startRow[] = getStartRow(indexEntry, cubeDescriptor.dimensions.size());
			byte stopRow[] = getStopRow(indexEntry, cubeDescriptor.dimensions.size());
			if(logger.isDebugEnabled()) logger.debug(methodName + "add range [" + LogUtils.describe(startRow)
					+ ", " + LogUtils.describe(stopRow) + "]  to scan.");
			Pair<byte[], byte[]> range = new Pair<byte[], byte[]>(startRow, stopRow);
			scan.getRanges().add(range);
		}
		return scan;
	}

	private byte[] getStopRow(CubeIndexEntry index, int size) {
		int rSize = (size + 1) * 8;
		byte r[] = new byte[rSize];
		Bytes.putBytes(r, 0, index.getData(), 0, index.getLength());
		for(int i = index.getLength(); i < rSize; i ++) 
			r[i] = Byte.MIN_VALUE;
		return r;
	}

	private byte[] getStartRow(CubeIndexEntry index, int size) {
		int rSize = (size + 1) * 8;
		byte r[] = new byte[rSize];
		Bytes.putBytes(r, 0, index.getData(), 0, index.getLength());
		for(int i = index.getLength(); i < rSize; i ++) 
			r[i] = 0;
		return r;
	}

	private List<CubeIndexEntry> joinIndexes(List<CubeIndexEntry> i1,
			List<CubeIndexEntry> i2) {
		List<CubeIndexEntry> r = new ArrayList<CubeIndexEntry>();
		for(CubeIndexEntry e1: i1) 
			for(CubeIndexEntry e2: i2) {
				boolean c1 = e1.contain(e2);
				boolean c2 = e2.contain(e1);
				if(c1 && c2)
					r.add(e1);
				if(c1 && !c2)
					r.add(e2);
				if(c2 && !c1)
					r.add(e1);
			}
		return r;
	}

	private List<CubeIndexEntry> mergeIndexes(List<CubeIndexEntry> i1,
			List<CubeIndexEntry> i2) {
		List<CubeIndexEntry> r1 = new ArrayList<CubeIndexEntry>();
		for(CubeIndexEntry e1: i1) {
			boolean contained = false;
			for(CubeIndexEntry e2: i2) 
				if(e2.contain(e1)) {
					contained = true;
					break;
				}
			if(!contained)
				r1.add(e1);
		}
		List<CubeIndexEntry> r2 = new ArrayList<CubeIndexEntry>();
		for(CubeIndexEntry e1: i2) {
			boolean contained = false;
			for(CubeIndexEntry e2: r1) 
				if(e2.contain(e1)) {
					contained = true;
					break;
				}
			if(!contained)
				r2.add(e1);
		}
		r1.addAll(r2);
		return r1;
	}

	private List<CubeIndexEntry> getIndexForDimensionValue(int dimensionNumber,
			long dimVal, CubeDescriptor cubeDescriptor) throws Exception {
		String methodName = "getIndexForDimensionValue() ";
		if(logger.isDebugEnabled()) logger.debug(methodName + "dimensionNumber = " + 
				dimensionNumber + " dimVal = " + dimVal);
		HTable hTable = new HTable(cubeDescriptor.getCubeIndexTable());
		byte key[] = DataUtils.pack(dimensionNumber, dimVal);
		if(logger.isDebugEnabled()) logger.debug(methodName + "index key = " + LogUtils.describe(key));
		Get get = new Get(key);
		byte indexColumn[] = Bytes.toBytes(OLAPEngineConstants.CUBE_INDEX_COLUMN);
		get.addColumn(indexColumn, indexColumn);
		byte index[] = hTable.get(get).getValue(indexColumn, indexColumn);
		if(logger.isDebugEnabled()) logger.debug(methodName + "index = " + LogUtils.describe(index));
		if(index == null)
			return new ArrayList<CubeIndexEntry>();
		DataInputBuffer buf = new DataInputBuffer();
		buf.reset(index, index.length);
		List<CubeIndexEntry> result = new ArrayList<CubeIndexEntry>();
		try {
			while(true) {
				CubeIndexEntry entry = new CubeIndexEntry();
				entry.readFields(buf);
				result.add(entry);
			}
		} catch(EOFException e) {
		}
		return result;
	}

	private int getDimensionNumber(String dimensionName,
			CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		for(int i = 0; i < cubeDescriptor.dimensions.size(); i ++) {
			if(cubeDescriptor.dimensions.get(i).getName().equals(dimensionName))
				return i + 1;
		}
		throw new OLAPEngineException("Can't find dimension " + dimensionName);
	}
	
	
}
