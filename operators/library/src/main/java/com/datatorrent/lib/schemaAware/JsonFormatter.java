package com.datatorrent.lib.schemaAware;

import java.util.Map;

import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

public class JsonFormatter extends com.datatorrent.lib.formatter.JsonFormatter implements SchemaAware
{
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    //No need to override this as there is not schema property of JsonFormatter that needs to be set
  }
}
