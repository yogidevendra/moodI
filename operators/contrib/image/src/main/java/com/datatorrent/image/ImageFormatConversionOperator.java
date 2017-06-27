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

import java.awt.HeadlessException;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.imageio.ImageIO;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ij.IJ;
import ij.ImagePlus;
import ij.io.Opener;
import ij.process.ImageProcessor;

/**
 * This operator uses ImageJ libraries for converting images to the desired file format.
 * The following image formats are supported jpeg,png,tiff,gif,fits,raw,avi,bmp.
 *
 * Usage:
 * Convert an image to and from any of the supported file formats.
 * by specifying the toFileType property in properties.xml
 *
 */
public class ImageFormatConversionOperator extends AbstractImageProcessingOperator
{
  @NotNull
  public String toFileType;

  public String getToFileType()
  {
    return toFileType;
  }

  /**
   * Sets the resultant image format.(eg. png)
   * @param toFileType
   */
  public void setToFileType(String toFileType)
  {
    this.toFileType = toFileType;
  }

  protected void converter(Data data)
  {
    String fromFileType = data.imageType;
    if (!fromFileType.contains("fit")) {
      bufferedImage = byteArrayToBufferedImage(data.bytesImage);
      ImagePlus imgPlus = new ImagePlus("source", bufferedImage);
      try {
        IJ.saveAs(imgPlus, toFileType, "");
      } catch (HeadlessException h) {
        throw new RuntimeException("imageJ is not running in headless mode" + h + "/n");
      }
      ImageProcessor imageProcessor = imgPlus.getProcessor();
      BufferedImage bufferedImage1;
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      try {
        bufferedImage1 = (BufferedImage)imageProcessor.createImage();
        ImageIO.write(bufferedImage1, toFileType, byteArrayOutputStream);
      } catch (IOException e) {
        throw new RuntimeException("Error in writing image to byteArray" + e);
      }
      data.bytesImage = byteArrayOutputStream.toByteArray();
      data.fileName = data.fileName.replace(fromFileType, toFileType);
      data.imageType = toFileType;
      try {
        byteArrayOutputStream.flush();
        byteArrayOutputStream.reset();
        byteArrayOutputStream.close();
        imageProcessor.reset();
      } catch (IOException e) {
        throw new RuntimeException("Error in closing byteArrayOutputStream" + e);
      }
      output.emit(data);
    } else {
      ImagePlus imgPlus = new Opener().deserialize(data.bytesImage);
      try {
        IJ.saveAs(imgPlus, toFileType, "");
      } catch (Exception e) {
        throw new RuntimeException("Error in saving image with imageJ  " + e);
      }
      ImageProcessor imageProcessor = imgPlus.getProcessor();
      BufferedImage bufferedImage1;
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      try {
        bufferedImage1 = (BufferedImage)imageProcessor.createImage();
        ImageIO.write(bufferedImage1, toFileType, byteArrayOutputStream);
      } catch (IOException e) {
        throw new RuntimeException("Error in writing image to byteArray" + e);
      }
      data.bytesImage = byteArrayOutputStream.toByteArray();
      data.fileName = data.fileName.replace(fromFileType, toFileType);
      data.imageType = toFileType;
      try {
        byteArrayOutputStream.flush();
        byteArrayOutputStream.reset();
        byteArrayOutputStream.close();
      } catch (IOException e) {
        throw new RuntimeException("Error in writing image to byteArray", e );
      }
      output.emit(data);
    }
  }

  @Override
  void processTuple(Data data)
  {
    converter(data);
  }
}
