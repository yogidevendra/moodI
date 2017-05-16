package com.datatorrent.lib.schemaAware;

import java.util.Map;

import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * Schema Aware JDBCOutputOperator
 */
public class JdbcOutputOperator extends JdbcPOJOInsertOutputOperator implements SchemaAware
{

  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {

  }

}
