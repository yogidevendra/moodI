package com.datatorrent.lib.schemaAware;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.validation.constraints.NotNull;

import com.datatorrent.contrib.parser.DelimitedSchema.FieldType;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * Schema aware Transform Operator
 */
public class TransformOperator extends com.datatorrent.lib.transform.TransformOperator implements SchemaAware
{

  @NotNull
  private Map<String, String> outputFieldMap = new HashMap<String, String>();

  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    if (outSchema.get(output) != null) {
      for (Entry<String, String> field : outputFieldMap.entrySet()) {
        outSchema.get(output).addField(field.getKey(), getClass(FieldType.valueOf(field.getValue())));
      }
    }
  }

  /**
   * Returns map of output fields and field type
   * 
   * @return outputFieldMap Map of outputFieldName ==> fieldType
   */
  public Map<String, String> getOutputFieldMap()
  {
    return outputFieldMap;
  }

  /**
   * Sets output field map which is a map containing information of field name
   * and its type. <br>
   * E.g. ["adId","INTEGER"],["adName","STRING"] <br>
   * Field types can be one of
   * <b> STRING,CHARACTER,INTEGER,LONG,SHORT,DOUBLE,FLOAT,BOOLEAN,DATE</b>.
   * 
   * @param outputFieldMap
   *          Map of String==>String where key is the field name and the value
   *          is field type.
   */
  public void setOutputFieldMap(Map<String, String> outputFieldMap)
  {
    this.outputFieldMap = outputFieldMap;
  }

  private Class getClass(FieldType type)
  {
    switch (type) {
      case CHARACTER:
        return Character.class;
      case STRING:
        return String.class;
      case INTEGER:
        return Integer.class;
      case LONG:
        return Long.class;
      case SHORT:
        return Short.class;
      case DOUBLE:
        return Double.class;
      case FLOAT:
        return Float.class;
      case BOOLEAN:
        return Boolean.class;
      case DATE:
        return Date.class;
      default:
        return null;
    }
  }

}
