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
package com.datatorrent.image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class BytesFileWriter extends AbstractFileOutputOperator<Data>
{
  private static final transient Logger LOG = LoggerFactory.getLogger(BytesFileWriter.class);

  public String fileName;
  private boolean eof;

  @Override
  protected byte[] getBytesForTuple(Data tuple)
  {
    eof = true;
    return tuple.bytesImage;
  }

  @Override
  protected String getFileName(Data tuple)
  {
    fileName = tuple.fileName;
    return tuple.fileName;
  }

  @Override
  public void endWindow()
  {

    if (!eof) {
      LOG.error("Error end of file not found " + fileName);
      return;
    }
    if (null == fileName) {
      LOG.error("Error file name is null" + fileName);
      return;
    }
    try {
      finalizeFile(fileName);
      Thread.sleep(100);
    } catch (Exception e) {
      LOG.error("Error in finalizing the file in Super class" + e);
    }
    super.endWindow();
    eof = false;
    fileName = null;
  }


}
