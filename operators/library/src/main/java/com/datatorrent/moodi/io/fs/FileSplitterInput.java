package com.datatorrent.moodi.io.fs;

import com.datatorrent.api.AutoMetric;

public class FileSplitterInput extends com.datatorrent.lib.io.fs.FileSplitterInput
{
  @AutoMetric
  protected int totalFilesRead = 0;

  @Override
  public void endWindow()
  {
    super.endWindow();
    totalFilesRead += filesProcessed;
  }
}
