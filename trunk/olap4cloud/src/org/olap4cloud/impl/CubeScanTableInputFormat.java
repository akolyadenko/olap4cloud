package org.olap4cloud.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;

public class CubeScanTableInputFormat extends TableInputFormat{
	
	private CubeScanTableRecordReader cubeScanTableRecordReader = null;
	
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
	        splits.addAll(generateSplits(splitStart, splitStop, regionLocation, cubeScan));	      
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
				InputSplit split = new CubeScanTableSplit(getHTable().getTableName(), splitStart, splitStop, regionLocation);
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
	    CubeScanTableSplit tSplit = (CubeScanTableSplit) split;
	    CubeScanTableRecordReader trr = this.cubeScanTableRecordReader;
	    // if no table record reader was provided use default
	    if (trr == null) {
	      trr = new CubeScanTableRecordReader();
	    }
	    Scan sc = new Scan(getScan());
	    sc.setStartRow(tSplit.getStartRow());
	    sc.setStopRow(tSplit.getEndRow());
	    trr.setScan(sc);
	    trr.setHTable(getHTable());
	    trr.init();
	    return trr;

	}
	
	  protected class CubeScanTableRecordReader
	  extends RecordReader<ImmutableBytesWritable, Result> {
	    
	    private ResultScanner scanner = null;
	    private Scan scan = null;
	    private HTable htable = null;
	    private byte[] lastRow = null;
	    private ImmutableBytesWritable key = null;
	    private Result value = null;

	    /**
	     * Restart from survivable exceptions by creating a new scanner.
	     *
	     * @param firstRow  The first row to start at.
	     * @throws IOException When restarting fails.
	     */
	    public void restart(byte[] firstRow) throws IOException {
	      Scan newScan = new Scan(scan);
	      newScan.setStartRow(firstRow);
	      this.scanner = this.htable.getScanner(newScan);
	    }

	    /**
	     * Build the scanner. Not done in constructor to allow for extension.
	     *
	     * @throws IOException When restarting the scan fails. 
	     */
	    public void init() throws IOException {
	      restart(scan.getStartRow());
	    }

	    /**
	     * Sets the HBase table.
	     * 
	     * @param htable  The {@link HTable} to scan.
	     */
	    public void setHTable(HTable htable) {
	      this.htable = htable;
	    }

	    /**
	     * Sets the scan defining the actual details like columns etc.
	     *  
	     * @param scan  The scan to set.
	     */
	    public void setScan(Scan scan) {
	      this.scan = scan;
	    }

	    /**
	     * Closes the split.
	     * 
	     * @see org.apache.hadoop.mapreduce.RecordReader#close()
	     */
	    @Override
	    public void close() {
	      this.scanner.close();
	    }

	    /**
	     * Returns the current key.
	     *  
	     * @return The current key.
	     * @throws IOException
	     * @throws InterruptedException When the job is aborted.
	     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	     */
	    @Override
	    public ImmutableBytesWritable getCurrentKey() throws IOException,
	        InterruptedException {
	      return key;
	    }

	    /**
	     * Returns the current value.
	     * 
	     * @return The current value.
	     * @throws IOException When the value is faulty.
	     * @throws InterruptedException When the job is aborted.
	     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
	     */
	    @Override
	    public Result getCurrentValue() throws IOException, InterruptedException {
	      return value;
	    }

	    /**
	     * Initializes the reader.
	     * 
	     * @param inputsplit  The split to work with.
	     * @param context  The current task context.
	     * @throws IOException When setting up the reader fails.
	     * @throws InterruptedException When the job is aborted.
	     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
	     *   org.apache.hadoop.mapreduce.InputSplit, 
	     *   org.apache.hadoop.mapreduce.TaskAttemptContext)
	     */
	    @Override
	    public void initialize(InputSplit inputsplit,
	        TaskAttemptContext context) throws IOException,
	        InterruptedException {
	    }

	    /**
	     * Positions the record reader to the next record.
	     *  
	     * @return <code>true</code> if there was another record.
	     * @throws IOException When reading the record failed.
	     * @throws InterruptedException When the job was aborted.
	     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	     */
	    @Override
	    public boolean nextKeyValue() throws IOException, InterruptedException {
	      if (key == null) key = new ImmutableBytesWritable();
	      if (value == null) value = new Result();
	      try {
	        value = this.scanner.next();
	      } catch (IOException e) {
	        logger.debug("recovered from " + StringUtils.stringifyException(e));  
	        restart(lastRow);
	        scanner.next();    // skip presumed already mapped row
	        value = scanner.next();
	      }
	      if (value != null && value.size() > 0) {
	        key.set(value.getRow());
	        lastRow = key.get();
	        return true;
	      }
	      return false;
	    }

	    /**
	     * The current progress of the record reader through its data.
	     * 
	     * @return A number between 0.0 and 1.0, the fraction of the data read.
	     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
	     */
	    @Override
	    public float getProgress() {
	      // Depends on the total number of tuples
	      return 0;
	    }
	  }

}
