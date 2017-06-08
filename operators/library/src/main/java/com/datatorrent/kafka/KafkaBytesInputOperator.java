package com.datatorrent.kafka;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.datatorrent.api.AutoMetric;

public class KafkaBytesInputOperator extends KafkaSinglePortInputOperator{

  /**
   * No. of messages emitted in the current window.
   */
  @AutoMetric
  private transient long numMessages;

  /**
   * No. of bytes emitted in the current window.
   */
  @AutoMetric
  private transient long numBytes;

  @Override
  public void beginWindow(long wid)
  {
    super.beginWindow(wid);
    numMessages = 0;
    numBytes = 0;
  }

  @Override
  protected void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message)
  {
    super.emitTuple(cluster, message);
    ++numMessages;
    numBytes += message.value().length;
  }
}
