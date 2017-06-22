package com.datatorrent.moodi.lib.io.db;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;

/**
 * JdbcPOJOPollInputOperator extends from com.datatorrent.lib.db.jdbc.JdbcPOJOPollInputOperator.
 * It emits the bytesReadPerSecond, bytesRead, totalEventsRead, eventsReadPerSecond metrics.
 *
 */
public class JdbcPOJOPollInputOperator extends com.datatorrent.lib.db.jdbc.JdbcPOJOPollInputOperator
{
  @AutoMetric
  private long totalEventsRead = 0;

  @AutoMetric
  private transient long eventsReadPerSecond;

  private transient long eventsReadPerWindow;
  private transient double windowTimeSec;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
      context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    eventsReadPerWindow = 0;
    eventsReadPerSecond = 0;
  }

  @Override
  protected void emitTuple(Object obj)
  {
    super.emitTuple(obj);
    eventsReadPerWindow++;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    eventsReadPerSecond = (long)(eventsReadPerWindow/windowTimeSec);
    totalEventsRead += eventsReadPerWindow;
  }
}

