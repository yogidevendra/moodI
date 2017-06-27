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

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.imageio.ImageIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This class handles the input and output of all the image processing operators under image.
 * It provides additional read/write methods and sets file path, file name, file type to make sure all operators are
 * compatible with each other.
 * All image processing operators should extend this class and override the processTuple(Data data) method.
 *
 */
public abstract class AbstractImageProcessingOperator extends BaseOperator
{
  public  String fileType;
  public final transient DefaultOutputPort<Data> output = new DefaultOutputPort<Data>();
  public String filePath;
  public transient BufferedImage bufferedImage = null;
  public int bufferedImageType;
  public final transient DefaultInputPort<Data> input = new DefaultInputPort<Data>()
  {

    @Override
    public void process(Data tuple)
    {
      filePath = tuple.fileName;
      fileType = tuple.imageType;
      processTuple(tuple);
    }
  };

  public BufferedImage byteArrayToBufferedImage(byte[] imageInBytes)
  {
    byte[] byteImage = imageInBytes;
    InputStream in = new ByteArrayInputStream(byteImage);
    try {
      bufferedImage = ImageIO.read(in);
      in.reset();
      in.close();
    } catch (IOException e) {
      throw new RuntimeException("Error in reading file " + e);
    }
    return bufferedImage;
  }

  public byte[] bufferedImageToByteArray(BufferedImage bufferedImage, String fileType)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ImageIO.write(bufferedImage,fileType, baos);
    } catch (IOException e) {
      throw new RuntimeException("Error in writing image to byte array " + e);
    }
    return baos.toByteArray();
  }

  abstract void processTuple(Data data);
}
