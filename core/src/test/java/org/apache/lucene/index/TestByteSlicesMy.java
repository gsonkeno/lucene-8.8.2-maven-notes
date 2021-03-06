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
package org.apache.lucene.index;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RecyclingByteBlockAllocator;

import java.util.Random;

public class TestByteSlicesMy extends LuceneTestCase {

  public void testBasic() throws Throwable {
    Random random = random();
    ByteBlockPool pool = new ByteBlockPool(new RecyclingByteBlockAllocator(ByteBlockPool.BYTE_BLOCK_SIZE, random().nextInt(100)));

    final int NUM_STREAM = atLeast(random, 100);

    ByteSliceWriter writer = new ByteSliceWriter(pool);
    //元素为pool.buffers中的起始地址
    int[] starts = new int[NUM_STREAM];
    //元素为pool.buffers中的结束地址
    int[] uptos = new int[NUM_STREAM];
    //元素值为写到pool.buffers从0,1,2.....n-1的最后一个数字
    int[] counters = new int[NUM_STREAM];

    //总体上看，以下标0为例，如果counters[0]=n,那么从pool的buffers的
    //地址start[0] 到地址uptos[0]，写入的就是0,1,2,3....n-1这些数字

    ByteSliceReader reader = new ByteSliceReader();

    for(int ti=0;ti<1;ti++) {

      for(int stream=0;stream<NUM_STREAM;stream++) {
        starts[stream] = -1;
        counters[stream] = 0;
      }
      
      int num = atLeast(random, 3000);
      for (int iter = 0; iter < num; iter++) {
        int stream;
        if (random.nextBoolean()) {
          stream = random.nextInt(3);
        } else {
          stream = random.nextInt(NUM_STREAM);
        }

        if (VERBOSE  && stream==0) {
          System.out.println("write stream=" + stream);
        }

        if (starts[stream] == -1) {
          final int spot = pool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
          starts[stream] = uptos[stream] = spot + pool.byteOffset;
          if (VERBOSE && stream==0) {
            System.out.println("  init to " + starts[stream]);
          }
        }

        writer.init(uptos[stream]);
        int numValue;
        if (random.nextInt(10) == 3) {
          numValue = random.nextInt(100);
        } else if (random.nextInt(5) == 3) {
          numValue = random.nextInt(3);
        } else {
          numValue = random.nextInt(20);
        }

        for(int j=0;j<numValue;j++) {
          if (VERBOSE && stream==0 ) {
            System.out.println("    write " + (counters[stream]+j));
          }
          // write some large (incl. negative) ints:
          writer.writeVInt(random.nextInt());
          writer.writeVInt(counters[stream]+j);
        }
        counters[stream] += numValue;
        uptos[stream] = writer.getAddress();
        if (VERBOSE && stream==0)
          System.out.println("    addr now " + uptos[stream]);
      }
    
      for(int stream=0;stream<NUM_STREAM;stream++) {
        if (VERBOSE && stream==0)
          System.out.println("  stream=" + stream + " count=" + counters[stream]);

        if (starts[stream] != -1 && starts[stream] != uptos[stream]) {
          reader.init(pool, starts[stream], uptos[stream]);
          for(int j=0;j<counters[stream];j++) {
            reader.readVInt();
            assertEquals(j, reader.readVInt()); 
          }
        }
      }

      pool.reset();
    }
  }
}
