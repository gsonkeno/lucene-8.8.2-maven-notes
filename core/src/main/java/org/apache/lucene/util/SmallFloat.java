/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util;

/** Floating point numbers smaller than 32 bits.
 *
 * @lucene.internal
 */
public class SmallFloat {
  
  /** No instance */
  private SmallFloat() {}

  /** Converts a 32 bit float to an 8 bit float.
   * <br>Values less than zero are all mapped to zero.
   * <br>Values are truncated (rounded down) to the nearest 8 bit value.
   * <br>Values between zero and the smallest representable value
   *  are rounded up.
   *
   * @param f the 32 bit float to be converted to an 8 bit float (byte)
   * @param numMantissaBits the number of mantissa bits to use in the byte, with the remainder to be used in the exponent
   * @param zeroExp the zero-point in the range of exponent values
   * @return the 8 bit float representation
   */
  public static byte floatToByte(float f, int numMantissaBits, int zeroExp) {
    // Adjustment from a float zero exponent to our zero exponent,
    // shifted over to our exponent position.
    int fzero = (63-zeroExp)<<numMantissaBits;
    int bits = Float.floatToRawIntBits(f);
    int smallfloat = bits >> (24-numMantissaBits);
    if (smallfloat <= fzero) {
      return (bits<=0) ?
        (byte)0   // negative numbers and zero both map to 0 byte
       :(byte)1;  // underflow is mapped to smallest non-zero number.
    } else if (smallfloat >= fzero + 0x100) {
      return -1;  // overflow maps to largest number
    } else {
      return (byte)(smallfloat - fzero);
    }
  }

  /** Converts an 8 bit float to a 32 bit float. */
  public static float byteToFloat(byte b, int numMantissaBits, int zeroExp) {
    // on Java1.5 & 1.6 JVMs, prebuilding a decoding array and doing a lookup
    // is only a little bit faster (anywhere from 0% to 7%)
    if (b == 0) return 0.0f;
    int bits = (b&0xff) << (24-numMantissaBits);
    bits += (63-zeroExp) << 24;
    return Float.intBitsToFloat(bits);
  }


  //
  // Some specializations of the generic functions follow.
  // The generic functions are just as fast with current (1.5)
  // -server JVMs, but still slower with client JVMs.
  //

  /** floatToByte(b, mantissaBits=3, zeroExponent=15)
   * <br>smallest non-zero value = 5.820766E-10
   * <br>largest value = 7.5161928E9
   * <br>epsilon = 0.125
   */
  public static byte floatToByte315(float f) {
    int bits = Float.floatToRawIntBits(f);
    int smallfloat = bits >> (24-3);
    if (smallfloat <= ((63-15)<<3)) {
      return (bits<=0) ? (byte)0 : (byte)1;
    }
    if (smallfloat >= ((63-15)<<3) + 0x100) {
      return -1;
    }
    return (byte)(smallfloat - ((63-15)<<3));
 }

  /** byteToFloat(b, mantissaBits=3, zeroExponent=15) */
  public static float byte315ToFloat(byte b) {
    // on Java1.5 & 1.6 JVMs, prebuilding a decoding array and doing a lookup
    // is only a little bit faster (anywhere from 0% to 7%)
    if (b == 0) return 0.0f;
    int bits = (b&0xff) << (24-3);
    bits += (63-15) << 24;
    return Float.intBitsToFloat(bits);
  }

  /** Float-like encoding for positive longs that preserves ordering and 4 significant bits.
   * 将一个long型正数编码为一个int型正数，且保证了原来的相对有序性
   * 返回结果[0,487]
   */
  public static int longToInt4(long i) {
    if (i < 0) {
      throw new IllegalArgumentException("Only supports positive values, got " + i);
    }
    // 表达i所需要的bit个数
    int numBits = 64 - Long.numberOfLeadingZeros(i);
    if (numBits < 4) {
      // subnormal value
      // 0-3个bit所能表示的long数字直接强转为int, i肯定在[0,7]之间
      return Math.toIntExact(i);
    } else {
      // normal value
      // [8-15] ,  numBit=4,  shift=0,   encoded=[1,7]|8=[9,15]
      // [16-17],  numBit=5,  shift=1,   encoded=[0,0]|16=[16,16]
      // [18-19],  numBit=6,  shift=1,   encoded=[1,1]|16=[17,17]
      // [20-21],  numBit=7,  shift=1,   encoded=[2,2]|16=[18,18]

      int shift = numBits - 4;
      // only keep the 5 most significant bits
      // 无符号右移，只保留高4位，且这高4位被无符号右移到最低位, 则encoded只有4个bit， encoded在[0-15]
      int encoded = Math.toIntExact(i >>> shift);
      // clear the most significant bit, which is implicit
      encoded &= 0x07; //只保留末尾3位了, 结果位[0,7]
      // encode the shift, adding 1 because 0 is reserved for subnormal values
      // shift范围[0,59],则(shift + 1) << 3范围为[8,480], 则encoded范围为[8,487]
      encoded |= (shift + 1) << 3;
      // 返回结果为[8,487]，数值的大小主要是靠(shift + 1) << 3影响
      return encoded;
    }
  }

  /**
   * Decode values encoded with {@link #longToInt4(long)}.
   */
  public static final long int4ToLong(int i) {
    long bits = i & 0x07;
    int shift = (i >>> 3) - 1;
    long decoded;
    if (shift == -1) {

      // subnormal value
     //  i处于[0,7]时，shift = -1, 返回i，输入=输出
      decoded = bits;
    } else {
      // normal value
      // bits永远处于[0,7], 或0x08相当于+8,   shift=i/8-1;
      // i越大，shift越大, decoded越大，正相关
      decoded = (bits | 0x08) << shift;
    }
    return decoded;
  }

  // 231
  private static final int MAX_INT4 = longToInt4(Integer.MAX_VALUE);
  // 24
  private static final int NUM_FREE_VALUES = 255 - MAX_INT4;

  /**
   * Encode an integer to a byte. It is built upon {@link #longToInt4(long)}
   * and leverages the fact that {@code longToInt4(Integer.MAX_VALUE)} is
   * less than 255 to encode low values more accurately.
   */
  public static byte intToByte4(int i) {
    if (i < 0) {
      throw new IllegalArgumentException("Only supports positive values, got " + i);
    }
    // NUM_FREE_VALUES = 24; 不足24原样返回
    if (i < NUM_FREE_VALUES) {
      return (byte) i;
    } else {
      // 已知longToInt4(Integer.MAX_VALUE) = 231
      // 不小于24, 则返回结果区间为[24, 24 + 231]，即[24,255]
      return (byte) (NUM_FREE_VALUES + longToInt4(i - NUM_FREE_VALUES));
    }
  }

  /**
   * Decode values that have been encoded with {@link #intToByte4(int)}.
   */
  public static int byte4ToInt(byte b) {
    int i = Byte.toUnsignedInt(b);
    if (i < NUM_FREE_VALUES) {
      return i;
    } else {
      long decoded = NUM_FREE_VALUES + int4ToLong(i - NUM_FREE_VALUES);
      return Math.toIntExact(decoded);
    }
  }
}
