package com.datatorrent.lib.schemaAware;

import java.util.Map;

import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.contrib.parser.DelimitedSchema.FieldType;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

public class JsonParser extends com.datatorrent.contrib.parser.JsonParser implements SchemaAware
{
  /**
   * String representing fields in the data and their type.<br>
   * A field name and its type is pipe-separated(|). Multiple fields are
   * separated by a comma (,).<br>
   * E.g
   * adId|INTEGER,adName|STRING,bidPrice|DOUBLE,active|BOOLEAN,weatherTargeted|
   * CHARACTER<br>
   * Possible values for data types are
   * <b>BOOLEAN,DOUBLE,INTEGER,FLOAT,LONG,SHORT,CHARACTER,STRING,DATE</b>
   */
  @NotNull
  private String fieldInfo;

  /**
   * Adds information of fields to <b>outSchema</b> for <b>out</b> port of
   * {@link JsonParser}
   */
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    if (outSchema.get(this.out) != null) {
      String[] fields = fieldInfo.split(",");
      for (String field : fields) {
        String[] field_tuple = field.split("\\|");
        outSchema.get(this.out).addField(field_tuple[0],
            SchemaAwareOperatorUtils.getClass(FieldType.valueOf(field_tuple[1])));
      }
    }
  }

  public String getFieldInfo()
  {
    return fieldInfo;
  }

  /**
   * Set the field info
   * 
   * @param fieldInfo
   *          String representing fields in the data and their type.<br>
   *          A field name and its type is pipe-separated(|). Multiple fields
   *          are separated by a comma (,).<br>
   *          E.g adId|INTEGER,adName|STRING,bidPrice|DOUBLE,active|BOOLEAN,
   *          weatherTargeted|CHARACTER<br>
   *          Possible values for data types are
   *          <b>BOOLEAN,DOUBLE,INTEGER,FLOAT,LONG,SHORT,CHARACTER,STRING,DATE
   *          </b>
   */
  public void setFieldInfo(String fieldInfo)
  {
    this.fieldInfo = fieldInfo;
  }

}
