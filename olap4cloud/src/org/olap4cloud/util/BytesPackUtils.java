package org.olap4cloud.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;


public class BytesPackUtils {
	public static byte[] pack(long l[]) {
		byte r[] = new byte[l.length * 8];
		for(int i = 0; i < l.length; i ++)
			Bytes.putLong(r, i * 8, l[i]);
		return r;
	}
	
	public static byte[] pack(int i, byte b[]) {
		byte r[] = new byte[b.length + 4];
		Bytes.putInt(r, 0, i);
		Bytes.putBytes(r, 4, b, 0, b.length);
		return r;
	}
	
	public static byte[] pack(int i, long l) {
		byte r[] = new byte[12];
		Bytes.putInt(r, 0, i);
		Bytes.putLong(r, 4, l);
		return r;
	}
	
	public static String objectToString(Object o) throws Exception {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream oout = new ObjectOutputStream(bout);
		oout.writeObject(o);
		oout.flush();
		bout.flush();
		return Base64.encodeBytes(bout.toByteArray());
	}
	
	public static Object stringToObject(String str) throws Exception {
		byte buf[] = Base64.decode(str);
		ByteArrayInputStream bin = new ByteArrayInputStream(buf);
		ObjectInputStream oin = new ObjectInputStream(bin);
		return oin.readObject();
	}
}
