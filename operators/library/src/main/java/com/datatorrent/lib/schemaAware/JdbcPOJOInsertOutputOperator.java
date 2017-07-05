package com.datatorrent.lib.schemaAware;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * Schema Aware JDBCOutputOperator
 */
@InterfaceStability.Evolving
public class JdbcPOJOInsertOutputOperator extends com.datatorrent.moodi.lib.io.db.JdbcPOJOInsertOutputOperator implements SchemaAware
{

  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {

  }

}
