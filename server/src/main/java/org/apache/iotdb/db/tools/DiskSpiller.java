/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import static org.weakref.jmx.internal.guava.util.concurrent.MoreExecutors.directExecutor;

public class DiskSpiller {

  private final List<Integer> fileIndex;
  private final List<TSDataType> dataTypeList;
  private final List<ListenableFuture<Boolean>> processingTask;
  private final String folderPath;
  private final String filePrefix;
  private final String fileSuffix = ".sortTemp";

  private int index;
  private boolean folderCreated = false;
  private boolean allProcessingTaskFinished = false;
  private final TsBlockSerde serde = new TsBlockSerde();

  public DiskSpiller(String folderPath, String filePrefix, List<TSDataType> dataTypeList) {
    this.folderPath = folderPath;
    this.filePrefix = filePrefix + this.getClass().getSimpleName() + "-";
    this.index = 0;
    this.dataTypeList = dataTypeList;
    this.fileIndex = new ArrayList<>();
    this.processingTask = new ArrayList<>();
  }

  public void createFolder(String folderPath) throws IOException {
    Path path = Paths.get(folderPath);
    Files.createDirectories(path);
    folderCreated = true;
  }

  public boolean allProcessingTaskFinished() throws IoTDBException {
    if (allProcessingTaskFinished) return true;
    for (Future<Boolean> future : processingTask) {
      if (!future.isDone()) return false;
      // check if there is exception in the processing task
      try {
        boolean finished = future.get();
        if (!finished) {
          throw new IoTDBException(
              "Failed to spill data to disk", TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        }
      } catch (Exception e) {
        throw new IoTDBException(
            e.getMessage(), TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
    }
    allProcessingTaskFinished = true;
    return true;
  }

  public synchronized void spill(List<TsBlock> tsBlocks) throws IOException {
    if (!folderCreated) {
      createFolder(folderPath);
    }
    String fileName = filePrefix + index + fileSuffix;
    fileIndex.add(index);
    index++;

    ListenableFuture<Boolean> future =
        Futures.submit(() -> writeData(tsBlocks, fileName), directExecutor());
    processingTask.add(future);
  }

  public void spillSortedData(List<MergeSortKey> sortedData) throws IOException {
    List<TsBlock> tsBlocks = new ArrayList<>();
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(dataTypeList);
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    ColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();

    for (MergeSortKey mergeSortKey : sortedData) {
      writeMergeSortKey(mergeSortKey, columnBuilders, timeColumnBuilder);
      tsBlockBuilder.declarePosition();
      if (tsBlockBuilder.isFull()) {
        tsBlocks.add(tsBlockBuilder.build());
        tsBlockBuilder.reset();
        timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
      }
    }

    if (!tsBlockBuilder.isEmpty()) {
      tsBlocks.add(tsBlockBuilder.build());
    }

    spill(tsBlocks);
  }

  private boolean writeData(List<TsBlock> sortedData, String fileName) {
    try {
      Path filePath = Paths.get(fileName);
      Files.createFile(filePath);

      try (FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {
        for (TsBlock tsBlock : sortedData) {
          ByteBuffer tsBlockBuffer = serde.serialize(tsBlock);
          ReadWriteIOUtils.write(tsBlockBuffer, fileOutputStream);
        }
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private void writeMergeSortKey(
      MergeSortKey mergeSortKey, ColumnBuilder[] columnBuilders, ColumnBuilder timeColumnBuilder) {
    timeColumnBuilder.writeLong(mergeSortKey.tsBlock.getTimeByIndex(mergeSortKey.rowIndex));
    for (int i = 0; i < columnBuilders.length; i++) {
      if (mergeSortKey.tsBlock.getColumn(i).isNull(mergeSortKey.rowIndex)) {
        columnBuilders[i].appendNull();
      } else {
        columnBuilders[i].write(mergeSortKey.tsBlock.getColumn(i), mergeSortKey.rowIndex);
      }
    }
  }

  public boolean hasSpilledData() {
    return !fileIndex.isEmpty();
  }

  public List<String> getFilePaths() {
    List<String> filePaths = new ArrayList<>();
    for (int index : fileIndex) {
      filePaths.add(filePrefix + index + fileSuffix);
    }
    return filePaths;
  }

  public List<SortReader> getReaders(SortBufferManager sortBufferManager)
      throws FileNotFoundException {
    List<String> filePaths = getFilePaths();
    List<SortReader> sortReaders = new ArrayList<>();
    for (String filePath : filePaths) {
      sortReaders.add(new FileSpillerReader(filePath, sortBufferManager, serde));
    }
    return sortReaders;
  }

  public void clear() throws IOException {
    List<String> filePaths = getFilePaths();
    for (String filePath : filePaths) {
      Path newPath = Paths.get(filePath);
      Files.deleteIfExists(newPath);
    }
  }
}
