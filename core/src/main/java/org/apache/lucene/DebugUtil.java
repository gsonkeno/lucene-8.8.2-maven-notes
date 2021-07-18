package org.apache.lucene;

import java.util.Arrays;

public class DebugUtil {
    public static boolean debug = true;

    public static void debug(String clazz, String action, long beginFp, long endFp, byte[] writtenBytes){
        if (debug){
            System.out.println("--------------");
            byte[] bytes = Arrays.copyOfRange(writtenBytes, (int)beginFp, (int)endFp);
            System.out.println(clazz + " " + action + " " +  beginFp + " " +
                    endFp + " " + (endFp - beginFp) + " " + Arrays.toString(bytes));
            System.out.println("--------------");
        }
    }

    public static byte[] getAddedBytes(byte[] oldBytes, byte[] newBytes){
        if (oldBytes == null){
            return newBytes;
        }

        int oldLength = oldBytes.length;
        int newLength  = newBytes.length;
        return Arrays.copyOfRange(newBytes, oldLength, newLength);
    }
}
