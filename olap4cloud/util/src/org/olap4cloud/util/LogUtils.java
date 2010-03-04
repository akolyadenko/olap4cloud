package org.olap4cloud.util;

public class LogUtils {
	public static String describe(byte b[]) {
		if(b == null)
			return "null";
		StringBuilder sb = new StringBuilder("{ ");
		for(int i = 0; i < b.length; i ++)
			sb.append(String.valueOf(b[i])).append(" ");
		sb.append(" }");
		return sb.toString();
	}
}
