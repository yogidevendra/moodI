package com.datatorrent.moodi.io.fs;

public class FSInputModule extends com.datatorrent.lib.io.fs.FSInputModule
{
  @Override
  public FileSplitterInput createFileSplitter()
  {
    return new FileSplitterInput();
  }

  @Override
  public FSSliceReader createBlockReader()
  {
    return new FSSliceReader();
  }
}
