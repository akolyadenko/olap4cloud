package org.olap4cloud.impl;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeQueryResult;
import org.olap4cloud.util.DataUtils;

public class CubeScanMR {

	static Logger logger = Logger.getLogger(CubeScanMR.class);
	
	public static CubeQueryResult scan(CubeScan scan, CubeDescriptor cubeDescriptor) throws Exception {
		String methodName = "CubeScanMR.scan() ";
		Job job = new Job();
		job.setJarByClass(CubeScanMR.class);
		TableMapReduceUtil.initTableMapperJob(cubeDescriptor.getCubeDataTable(), scan.getHBaseScan()
				, CubeScanMRMapper.class
				, ImmutableBytesWritable.class, DoubleWritable.class, job);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(CubeScanMRReducer.class);
		job.setCombinerClass(CubeScanMRReducer.class);
		String outPath = OLAPEngineConstants.MR_OUT_DIRECTORY_PREFIX + System.currentTimeMillis();
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		job.getConfiguration().set(OLAPEngineConstants.JOB_CONF_PROP_CUBE_DESCRIPTOR
				, DataUtils.objectToString(cubeDescriptor));
		job.getConfiguration().set(OLAPEngineConstants.JOB_CONF_PROP_CUBE_QUERY
				, DataUtils.objectToString(scan));
		job.waitForCompletion(true);
		return null;
	}
	
	
	
	public static class CubeScanMRMapper extends TableMapper<ImmutableBytesWritable, DoubleWritable>{
		
		CubeDescriptor cubeDescriptor = null;
		
		CubeScan cubeScan = null;
		
		@Override
		protected void setup(Mapper<ImmutableBytesWritable,Result,ImmutableBytesWritable
				,DoubleWritable>.Context context) throws IOException ,InterruptedException {
			try {
				cubeDescriptor = (CubeDescriptor)DataUtils.stringToObject(context.getConfiguration()
						.get(OLAPEngineConstants.JOB_CONF_PROP_CUBE_DESCRIPTOR));
				cubeScan = (CubeScan)DataUtils.stringToObject(context.getConfiguration()
						.get(OLAPEngineConstants.JOB_CONF_PROP_CUBE_QUERY));
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new InterruptedException(e.getMessage());
			}
		};
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			
		}
	}
	
	public static class CubeScanMRReducer extends Reducer<ImmutableBytesWritable, DoubleWritable, ImmutableBytesWritable
	, DoubleWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable inKey,
				Iterable<DoubleWritable> inVal,
				org.apache.hadoop.mapreduce.Reducer<ImmutableBytesWritable, DoubleWritable
				, ImmutableBytesWritable
				, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			
		}
		
	}
}
