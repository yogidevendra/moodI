package com.datatorrent.image;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Operator can be used to compress lossy images (example: jpeg).
 * Compression quality can be set by assigning desired float value to the compressionRatio property in properties.xml
 * 1.0f is the maximum quality image i.e lowest compression.
 * 0.0f is the minimum quality image i.e maximum compression.
 */
public class ImageCompressionOperator extends AbstractImageProcessingOperator
{
  @NotNull
  public float compressionRatio;

  public float getCompressionRatio()
  {
    return compressionRatio;
  }

  /**
   * Sets compressionRatio value set in properties.xml
   * @param compressionRatio
   */
  public void setCompressionRatio(float compressionRatio)
  {
    this.compressionRatio = compressionRatio;
  }

  public void compress(Data data)
  {
    if (data != null) {
      bufferedImage = byteArrayToBufferedImage(data.bytesImage);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName(data.imageType);
      ImageWriter writer = writers.next();
      writer.setOutput(new MemoryCacheImageOutputStream(baos));
      ImageWriteParam param = writer.getDefaultWriteParam();
      if (param.canWriteCompressed()) {
        param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        param.setCompressionQuality(compressionRatio);
      }
      try {
        writer.write(null, new IIOImage(bufferedImage, null, null), param);
        writer.dispose();
        data.bytesImage = baos.toByteArray();
        baos.flush();
        baos.reset();
        baos.close();
        output.emit(data);
      } catch (IOException e) {
      throw new RuntimeException("Error in writing image to byte array" + e.getMessage());
      }
    }
  }

  @Override
  void processTuple(Data data)
  {
    compress(data);
  }
}
