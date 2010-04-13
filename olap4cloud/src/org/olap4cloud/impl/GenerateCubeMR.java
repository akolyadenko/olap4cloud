package org.olap4cloud.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.util.DataUtils;

public class GenerateCubeMR {
	
	public static class GenerateCubeMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		
		static Logger logger = Logger.getLogger(GenerateCubeMapper.class);
		
		CubeDescriptor cubeDescriptor;
		
		int dimensionN;
		
		int measureN;
		
		byte measureNames[][];
		
		byte measureFamilies[][];
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			try {
				cubeDescriptor = (CubeDescriptor)DataUtils.stringToObject(context.getConfiguration()
					.get(OLAPEngineConstants.JOB_CONF_PROP_CUBE_DESCRIPTOR));
				dimensionN = cubeDescriptor.getDimensions().size();
				int measureN = cubeDescriptor.getMeasures().size();
				measureNames = new byte[measureN][];
				measureFamilies = new byte[measureN][];
				for(int i = 0; i < measureN; i ++) {
					measureNames[i] = Bytes.toBytes(cubeDescriptor.getMeasures().get(i).getName());
					measureFamilies[i] = Bytes.toBytes(OLAPEngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX 
							+ cubeDescriptor.getMeasures().get(i).getName());
				}
			} catch(Exception e) {
				logger.error(e.getMessage(), e);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			long dimensions[] = new long[dimensionN + 1];
			String tokens[] = value.toString().split("\t");
			for(int i = 0; i < dimensionN; i ++)
				dimensions[i] = Long.parseLong(tokens[i]);
			dimensions[dimensions.length - 1] = key.get();
			byte cubeKey[] = DataUtils.pack(dimensions);
			Put put = new Put(cubeKey);
			for(int i = 0; i < measureN; i ++) {		
				put.add(measureFamilies[i], measureNames[i], 
						Bytes.toBytes(Double.parseDouble(tokens[dimensionN + i])));
			}
			context.write(new ImmutableBytesWritable(cubeKey), put);
		}
	}
	
	public static void generateCube(CubeDescriptor descr) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescr = new HTableDescriptor(descr.getCubeDataTable());
		for(int i = 0; i < descr.getMeasures().size(); i ++)
			tableDescr.addFamily(new HColumnDescriptor(OLAPEngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX 
					+ descr.getMeasures().get(i).getName()));
		if(admin.tableExists(descr.getCubeDataTable())) {
			admin.disableTable(descr.getCubeDataTable());
			admin.deleteTable(descr.getCubeDataTable());
		}
		admin.createTable(tableDescr);
		Job job = new Job();
		job.setJarByClass(GenerateCubeMR.class);
		job.setMapperClass(GenerateCubeMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(descr.getSourceDataDir()));
		TableMapReduceUtil.initTableReducerJob(descr.getCubeDataTable()
				, IdentityTableReducer.class, job);
		job.waitForCompletion(true);
	}
}
