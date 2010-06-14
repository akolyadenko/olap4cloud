package org.olap4cloud.util;

import org.olap4cloud.impl.CubeIndexEntry;

public class LogUtils {
	public static String describe(byte b[]) {
		if(b == null)
			return "null";
		StringBuffer sb = new StringBuffer("{ ");
		for(int i = 0; i < b.length; i ++)
			sb.append(String.valueOf(b[i])).append(" ");
		sb.append(" }");
		return sb.toString();
	}
	
	public static String describe(double b[]) {
		if(b == null)
			return "null";
		StringBuffer sb = new StringBuffer("{ ");
		for(int i = 0; i < b.length; i ++)
			sb.append(String.valueOf(b[i])).append(" ");
		sb.append(" }");
		return sb.toString();
	}
	
	public static String describe(CubeIndexEntry e) {
		return "IndexEntry{length = " + e.getLength() + ", data = " + describe(e.getData()) + "}";
	}
}
