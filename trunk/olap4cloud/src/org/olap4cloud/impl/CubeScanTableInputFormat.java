package org.olap4cloud.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

public class CubeScanTableInputFormat extends TableInputFormat{
	
	static Logger logger = Logger.getLogger(CubeScanTableInputFormat.class);
	
	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException {
	    Pair<byte[][], byte[][]> keys = getHTable().getStartEndKeys();
	    if (keys == null || keys.getFirst() == null || 
	        keys.getFirst().length == 0) {
	      throw new IOException("Expecting at least one region.");
	    }
	    if (getHTable() == null) {
	      throw new IOException("No table was provided.");
	    }
	    int count = 0;
	    List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length); 
	    for (int i = 0; i < keys.getFirst().length; i++) {
	      String regionLocation = getHTable().getRegionLocation(keys.getFirst()[i]).
	        getServerAddress().getHostname();
	      byte[] startRow = getScan().getStartRow();
	      byte[] stopRow = getScan().getStopRow();
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
	      }
	    }
	    return splits;

	}
}