package org.olap4cloud.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.hbase.util.Bytes;

public class BytesPackUtils {
	public static byte[] pack(long l[]) {
		byte r[] = new byte[l.length * 8];
		byte b[] = null;
		for(int i = 0; i < l.length; i ++) {
			b = Bytes.toBytes(l[i]);
			for(int j = 0; j < 8; j ++)
				r[i * 8 + j] = b[j];
		}
		return r;
	}
}
