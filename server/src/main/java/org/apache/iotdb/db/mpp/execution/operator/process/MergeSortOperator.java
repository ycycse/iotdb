package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.MergeSortToolKit;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class MergeSortOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final List<Operator> inputOperators;
  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;
  private final int inputOperatorsCount;
  private final TsBlock[] inputTsBlocks;
  private final boolean[] noMoreTsBlocks;
  private boolean finished;

  private final MergeSortToolKit mergeSortToolKit;

  public MergeSortOperator(
      OperatorContext operatorContext,
      List<Operator> inputOperators,
      List<TSDataType> dataTypes,
      MergeSortToolKit mergeSortToolKit) {
    this.operatorContext = operatorContext;
    this.inputOperators = inputOperators;
    this.dataTypes = dataTypes;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.inputOperatorsCount = inputOperators.size();
    this.inputTsBlocks = new TsBlock[inputOperatorsCount];
    this.noMoreTsBlocks = new boolean[inputOperatorsCount];
    this.mergeSortToolKit = mergeSortToolKit;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && isTsBlockEmpty(i)) {
        ListenableFuture<?> blocked = inputOperators.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutures.add(blocked);
        }
      }
    }
    return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
  }

  /**
   * If the tsBlock of tsBlockIndex is null or has no more data in the tsBlock, return true; else
   * return false;
   */
  private boolean isTsBlockEmpty(int tsBlockIndex) {
    return inputTsBlocks[tsBlockIndex] == null
        || inputTsBlocks[tsBlockIndex].getPositionCount() == 0;
  }

  @Override
  public TsBlock next() {
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && isTsBlockEmpty(i) && inputOperators.get(i).hasNext()) {
        inputTsBlocks[i] = inputOperators.get(i).next();
        if (inputTsBlocks[i] == null || inputTsBlocks[i].isEmpty()) {
          return null;
        }
        mergeSortToolKit.addTsBlock(inputTsBlocks[i], i);
      }
    }

    List<Integer> targetTsBlockIndex = mergeSortToolKit.getTargetTsBlockIndex();
    int targetTsBlockSize = targetTsBlockIndex.size();
    if (targetTsBlockSize == 1) {
      int index = targetTsBlockIndex.get(0);
      TsBlock resultTsBlock = inputTsBlocks[index];
      inputTsBlocks[index] = null;
      return resultTsBlock;
    }

    // get the row of tsBlock whose keyValue <= targetValue
    tsBlockBuilder.reset();

    TsBlock.TsBlockSingleColumnIterator[] tsBlockIterators =
        new TsBlock.TsBlockSingleColumnIterator[targetTsBlockSize];
    for (int i = 0; i < targetTsBlockSize; i++) {
      tsBlockIterators[i] =
          inputTsBlocks[targetTsBlockIndex.get(i)].getTsBlockSingleColumnIterator();
    }

    // use the min KeyValue of all TsBlock as the end keyValue of result tsBlock
    boolean hasMatchKey = true;
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    while (hasMatchKey) {
      int minIndex = -1;
      hasMatchKey = false;
      // find the targetTsBlock which has the smallest keyValue
      for (int i = 1; i < targetTsBlockSize; i++) {
        if (mergeSortToolKit.satisfyCurrentEndValue(tsBlockIterators[i])) {
          hasMatchKey = true;
          if (minIndex == -1) {
            minIndex = i;
          } else if (mergeSortToolKit.greater(tsBlockIterators[minIndex], tsBlockIterators[i])) {
            minIndex = i;
          }
        }
      }
      // add to result TsBlock
      if (hasMatchKey) {
        if (tsBlockIterators[minIndex].hasNext()) {
          int rowIndex = tsBlockIterators[minIndex].getRowIndex();
          TsBlock targetBlock = inputTsBlocks[targetTsBlockIndex.get(minIndex)];
          timeBuilder.writeLong(targetBlock.getTimeByIndex(rowIndex));
          for (int i = 0; i < valueColumnBuilders.length; i++) {
            if (targetBlock.getColumn(i).isNull(rowIndex)) {
              valueColumnBuilders[i].appendNull();
              continue;
            }
            valueColumnBuilders[i].write(targetBlock.getColumn(i), rowIndex);
          }
        }
        tsBlockIterators[minIndex].next();
      }
      tsBlockBuilder.declarePosition();
    }
    // update inputTsBlocks after consuming
    for (int i = 0; i < targetTsBlockSize; i++) {
      if (tsBlockIterators[i].hasNext()) {
        int rowIndex = tsBlockIterators[i].getRowIndex();
        inputTsBlocks[targetTsBlockIndex.get(i)] =
            inputTsBlocks[targetTsBlockIndex.get(i)].subTsBlock(rowIndex);
        mergeSortToolKit.updateTsBlock(i,rowIndex);
      } else {
        inputTsBlocks[targetTsBlockIndex.get(i)] = null;
        mergeSortToolKit.updateTsBlock(i,-1);
      }
    }
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!isTsBlockEmpty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (inputOperators.get(i).hasNext()) {
          return true;
        } else {
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      }
    }
    return false;
  }

  @Override
  public void close() throws Exception {
    for (Operator operator : inputOperators) {
      operator.close();
    }
  }

  @Override
  public boolean isFinished() {
    if (finished) {
      return true;
    }
    finished = true;

    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] || !isTsBlockEmpty(i)) {
        finished = false;
        break;
      }
    }
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return (1L + dataTypes.size()) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }
}
