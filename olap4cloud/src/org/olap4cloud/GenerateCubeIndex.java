package org.olap4cloud;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.olap4cloud.GenerateCube.GenerateCubeMapper;
import org.olap4cloud.util.BytesPackUtils;

public class GenerateCubeIndex {
	public static class GenerateCubeIndexMapper extends TableMapper<ImmutableBytesWritable, CubeIndexEntry> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			byte keyBytes[] = key.get();
			int len = (keyBytes.length - 16) / 8;
			for(int i = 0; i < len; i ++) {
				int dimNumber = i + 1;
				byte keyData[] = Arrays.copyOfRange(keyBytes, (i + 1) * 8, (i + 2) * 8);
				byte outKey[] = BytesPackUtils.pack(dimNumber, keyData);
				int indexLength = (i + 1) * 8;
				byte indexData[] = Arrays.copyOfRange(keyBytes, 0, (i + 1) * 8);
				context.write(new ImmutableBytesWritable(outKey), new CubeIndexEntry(indexLength, indexData));
			} 
		}
	}
	
	public static class GenerateCubeIndexReducer 
		extends TableReducer<ImmutableBytesWritable, CubeIndexEntry, ImmutableBytesWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable inKey,
				Iterable<CubeIndexEntry> vals,
				Context context)
				throws IOException, InterruptedException {
			Set<CubeIndexEntry> index = new TreeSet<CubeIndexEntry>();
			for(Iterator<CubeIndexEntry> i = vals.iterator(); i.hasNext(); ) 
				index.add(i.next());
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			DataOutputStream dout = new DataOutputStream(bout);
			for(Iterator<CubeIndexEntry> i = index.iterator(); i.hasNext();)
				i.next().write(dout);
			dout.close();
			bout.close();
			byte indexData[] = bout.toByteArray();
			Put put = new Put(inKey.get());
			put.add(Bytes.toBytes(EngineConstants.CUBE_INDEX_COLUMN), Bytes.toBytes(EngineConstants.CUBE_INDEX_COLUMN),
					indexData);
			context.write(inKey, put);
		}
	}
	
	public static void generate(CubeDescriptor descr) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescr = new HTableDescriptor(descr.getCubeIndexTable());
		tableDescr.addFamily(new HColumnDescriptor(EngineConstants.CUBE_INDEX_COLUMN));
		admin.deleteTable(Bytes.toBytes(descr.getCubeIndexTable()));
		admin.createTable(tableDescr);
		Job job = new Job();
		job.setJarByClass(GenerateCubeIndex.class);
		TableMapReduceUtil.initTableMapperJob(descr.getCubeDataTable(), new Scan(), GenerateCubeIndexMapper.class
				, ImmutableBytesWritable.class, CubeIndexEntry.class, job);
		TableMapReduceUtil.initTableReducerJob(descr.getCubeIndexTable()
				, GenerateCubeIndexReducer.class, job);
		job.waitForCompletion(true);
	}
}
