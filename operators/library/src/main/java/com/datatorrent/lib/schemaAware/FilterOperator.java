package com.datatorrent.lib.schemaAware;

import java.util.Map;
import java.util.Map.Entry;

import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * Schema Aware Filter Operator
 */
public class FilterOperator extends com.datatorrent.lib.filter.FilterOperator implements SchemaAware
{

  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    Schema inputSchema = inSchema.get(input);
    for (Entry<String, Class> field : inputSchema.getFieldList().entrySet()) {
      if (outSchema.get(truePort) != null) {
        outSchema.get(truePort).addField(field.getKey(), field.getValue());
      }
      if (outSchema.get(falsePort) != null) {
        outSchema.get(falsePort).addField(field.getKey(), field.getValue());
      }
      if (outSchema.get(error) != null) {
        outSchema.get(error).addField(field.getKey(), field.getValue());
      }
    }
  }

}
