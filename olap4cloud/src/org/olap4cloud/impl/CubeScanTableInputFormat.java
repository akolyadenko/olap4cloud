package org.olap4cloud.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;

public class CubeScanTableInputFormat extends TableInputFormat{
	
	static Logger logger = Logger.getLogger(CubeScanTableInputFormat.class);
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
	    Pair<byte[][], byte[][]> keys = getHTable().getStartEndKeys();
	    if (keys == null || keys.getFirst() == null || 
	        keys.getFirst().length == 0) {
	      throw new IOException("Expecting at least one region.");
	    }
	    if (getHTable() == null) {
	      throw new IOException("No table was provided.");
	    }
	    CubeScan cubeScan = null;
	    try {
	    	cubeScan = (CubeScan)DataUtils.stringToObject(context.getConfiguration()
	    			.get(OLAPEngineConstants.JOB_CONF_PROP_CUBE_SCAN));
	    } catch(Exception e) {
	    	logger.error(e.getMessage(), e);
	    	throw new IOException(e);
	    }
	    int count = 0;
	    List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length); 
	    for (int i = 0; i < keys.getFirst().length; i++) {
	      String regionLocation = getHTable().getRegionLocation(keys.getFirst()[i]).
	        getServerAddress().getHostname();
	      byte[] startRow = getScan().getStartRow();
	      byte[] stopRow = getScan().getStopRow();
	      if(!acceptRange(startRow, stopRow, cubeScan))
	    	  continue;
	      // determine if the given start an stop key fall into the region
	      if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
	           Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
	          (stopRow.length == 0 || 
	           Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
	        byte[] splitStart = startRow.length == 0 || 
	          Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? 
	            keys.getFirst()[i] : startRow;
	        byte[] splitStop = (stopRow.length == 0 || 
	          Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
	          keys.getSecond()[i].length > 0 ? 
	            keys.getSecond()[i] : stopRow;
	        InputSplit split = new TableSplit(getHTable().getTableName(),
	          splitStart, splitStop, regionLocation);
	        splits.add(split);
	        if (logger.isDebugEnabled()) 
	          logger.debug("getSplits: split -> " + (count++) + " -> " + split);
//	        splits.addAll(generateSplits(splitStart, splitStop, regionLocation, cubeScan));	      
	      }
	    }
	    return splits;

	}

	private List<InputSplit> generateSplits(byte[] regionStart, byte[] regionStop,
			String regionLocation, CubeScan cubeScan) {
		List<InputSplit> r = new ArrayList<InputSplit>();
		for(Pair<byte[], byte[]> range: cubeScan.getRanges()) {
			if(DataUtils.compareRowKeys(regionStart, range.getSecond()) <= 0 
					&& DataUtils.compareRowKeys(regionStop, range.getFirst()) >= 0) {
				byte splitStart[] = null;
				byte splitStop[] = null;
				if(DataUtils.compareRowKeys(regionStart, range.getFirst()) > 0)
					splitStart = regionStart;
				else
					splitStart = range.getFirst();
				if(DataUtils.compareRowKeys(regionStop, range.getSecond()) < 0)
					splitStop = regionStop;
				else
					splitStop = range.getSecond();
				InputSplit split = new TableSplit(getHTable().getTableName(), splitStart, splitStop, regionLocation);
				r.add(split);
				if (logger.isDebugEnabled()) 
			          logger.debug("getSplits: split -> " + split);
			}
		}
		return r;
	}

	private boolean acceptRange(byte[] startRow, byte[] stopRow,
			CubeScan cubeScan) {
		String methodName = "acceptRange() ";
		for(Pair<byte[], byte[]> range: cubeScan.getRanges()) {
			if(logger.isDebugEnabled()) logger.debug(methodName + "check startRow = " 
					+ LogUtils.describe(startRow) + ", stopRow = " + LogUtils.describe(stopRow)
					+ " and range = [" + LogUtils.describe(range.getFirst()) + ", " 
					+ LogUtils.describe(range.getSecond()) + "]");
			if(DataUtils.compareRowKeys(startRow, range.getSecond()) <= 0 
					&& DataUtils.compareRowKeys(stopRow, range.getFirst()) >= 0)
				return true;
		}
		if(logger.isDebugEnabled()) logger.debug(methodName + "start and stop rows don't match any range.");
		return false;
	}
	
	@Override
	public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		String methodName = "createRecordReader() ";
		logger.debug(methodName + "split = " +  split);
		return super.createRecordReader(split, context);
	}
}
