package com.datatorrent.moodi.io.fs;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;

public class FSSliceReader extends com.datatorrent.lib.io.block.FSSliceReader
{
  @AutoMetric
  protected long totalBlocksRead = 0;

  @AutoMetric
  protected long totalBytesRead = 0;

  @AutoMetric
  protected transient long bytesReadPerSecond = 0;
  private transient double windowTimeSec;
  private transient long blockSize = 0;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
      context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    totalBlocksRead = counters.getCounter(ReaderCounterKeys.BLOCKS).longValue();
    totalBytesRead = counters.getCounter(ReaderCounterKeys.BYTES).longValue();
    if (totalBlocksRead != 0 && totalBytesRead != 0) {
      blockSize = totalBytesRead / totalBlocksRead;
    }
    bytesReadPerSecond = (long)((blocksPerWindow * blockSize)/windowTimeSec);
  }
}
