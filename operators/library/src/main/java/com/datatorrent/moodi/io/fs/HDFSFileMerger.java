package com.datatorrent.moodi.io.fs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.AutoMetric;

public class HDFSFileMerger extends com.datatorrent.lib.io.fs.HDFSFileMerger
{

  @AutoMetric
  protected int totalCompletedFiles = 0;

  @Override
  protected void moveToFinalFile(Path tempOutFilePath, Path destination) throws IOException
  {
    super.moveToFinalFile(tempOutFilePath, destination);
    totalCompletedFiles++;
  }
}
