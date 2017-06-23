/**
* LIMITED LICENSE
* THE TERMS OF THIS LIMITED LICENSE (?AGREEMENT?) GOVERN YOUR USE OF THE SOFTWARE, DOCUMENTATION AND ANY OTHER MATERIALS MADE
* AVAILABLE ON THIS SITE (?LICENSED MATERIALS?) BY DATATORRENT.  ANY USE OF THE LICENSED MATERIALS IS GOVERNED BY THE FOLLOWING
* TERMS AND CONDITIONS.  IF YOU DO NOT AGREE TO THE FOLLOWING TERMS AND CONDITIONS, YOU DO NOT HAVE THE RIGHT TO DOWNLOAD OR
* VIEW THE LICENSED MATERIALS.  

* Under this Agreement, DataTorrent grants to you a personal, limited, non-exclusive, non-assignable, non-transferable
*  non-sublicenseable, revocable right solely to internally view and evaluate the Licensed Materials. DataTorrent reserves
*  all rights not expressly granted in this Agreement. 
* Under this Agreement, you are not granted the right to install or operate the Licensed Materials. To obtain a license
* granting you a license with rights beyond those granted under this Agreement, please contact DataTorrent at www.datatorrent.com. 
* You do not have the right to, and will not, reverse engineer, combine, modify, adapt, copy, create derivative works of,
* sublicense, transfer, distribute, perform or display (publicly or otherwise) or exploit the Licensed Materials for any purpose
* in any manner whatsoever.
* You do not have the right to, and will not, use the Licensed Materials to create any products or services which are competitive
* with the products or services of DataTorrent.
* The Licensed Materials are provided to you 'as is' without any warranties. DATATORRENT DISCLAIMS ANY AND ALL WARRANTIES, EXPRESS
* OR IMPLIED, INCLUDING THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, TITLE AND NON-INFRINGEMENT AND ANY
* WARRANTIES ARISING FROM A COURSE OR PERFORMANCE, COURSE OF DEALING OR USAGE OF TRADE.  DATATORRENT AND ITS LICENSORS SHALL NOT
* BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF
* OR IN CONNECTION WITH THE LICENSED MATERIALS.
 */
package com.datatorrent.moodi.lib.nosql.cassandra.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.batch.BatchControlTuple;
import com.datatorrent.batch.Batchable;
import com.datatorrent.contrib.cassandra.CassandraPOJOInputOperator;

/**
 * <p>
 * CassandraPOJOInputOperator
 * </p>
 * A generic implementation of AbstractCassandraInputOperator that fetches rows
 * of data from Cassandra and emits them as POJOs. Each row is converted to a
 * POJO by mapping the columns in the row to fields of the POJO based on a user
 * specified mapping. User should also provide a query to fetch the rows from
 * database. This query is run continuously to fetch new data and hence should
 * be parameterized. The parameters that can be used are %t for table name, %p
 * for primary key, %s for start value and %l for limit. The start value is
 * continuously updated with the value of a primary key column of the last row
 * from the result of the previous run of the query. The primary key column is
 * also identified by the user using a property. Before emitting any tuple, a
 * startBatch control tuple is emitted indicating start of a batch. Once all
 * tuples are emitted, we end the batch by sending the end batch control tuple.
 * The operator then requests a shutdown of the application as there is no more
 * data to process.
 *
 * @displayName Cassandra Input Operator
 * @category Input
 * @tags database, nosql, pojo, cassandra, batch
 */

@Evolving
public class CassandraPojoBatchInputOperator extends CassandraPOJOInputOperator
    implements Operator.CheckpointNotificationListener, Batchable
{
  protected boolean startBatchEmitted = false;
  protected boolean endBatchEmitted = false;
  protected transient long recordsEmittedInCurrentWindow;
  private long shutdownWindowId = -1;
  private transient long currentWindowId;

  public final transient ControlAwareDefaultOutputPort<Object> outputPort = new ControlAwareDefaultOutputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
    recordsEmittedInCurrentWindow = 0;
  }

  @Override
  public void emitTuples()
  {
    if (shouldEmitStartBatch()) {
      emitStartBatchControlTuple();
      startBatchEmitted = true;
    }
    super.emitTuples();
  }

  @Override
  protected void emit(Object tuple)
  {
    super.emit(tuple);
    recordsEmittedInCurrentWindow++;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (shouldEmitEndBatch()) {
      emitEndBatchControlTuple();
      endBatchEmitted = true;
      shutdownWindowId = this.currentWindowId;
    }
  }

  @Override
  public void emitStartBatchControlTuple()
  {
    if (getOutputPort() instanceof ControlAwareDefaultOutputPort) {
      BatchControlTuple startBatchControlTuple = new BatchControlTuple.StartBatchControlTupleImpl();
      ControlAwareDefaultOutputPort<Object> output = (ControlAwareDefaultOutputPort<Object>)getOutputPort();
      output.emitControl(startBatchControlTuple);
    } else {
      LOG.error("Output port is not control aware, skipped emitting start batch control tuple");
    }
  }

  @Override
  public void emitEndBatchControlTuple()
  {
    if (getOutputPort() instanceof ControlAwareDefaultOutputPort) {
      BatchControlTuple endBatchControlTuple = new BatchControlTuple.EndBatchControlTupleImpl();
      ControlAwareDefaultOutputPort<Object> output = (ControlAwareDefaultOutputPort<Object>)getOutputPort();
      output.emitControl(endBatchControlTuple);
    } else {
      LOG.error("Output port is not control aware, skipped emitting end batch control tuple");
    }
  }

  protected boolean shouldEmitStartBatch()
  {
    return !startBatchEmitted;
  }

  protected boolean shouldEmitEndBatch()
  {
    //If no tuples were emitted in the emitTuples call
    return (!endBatchEmitted && recordsEmittedInCurrentWindow == 0);
  }

  protected DefaultOutputPort<Object> getOutputPort()
  {
    return this.outputPort;
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    //Shutdown the application after all records are read.
    if (shutdownWindowId != -1 && windowId > shutdownWindowId) {
      throw new ShutdownException();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(CassandraPojoBatchInputOperator.class);

}
