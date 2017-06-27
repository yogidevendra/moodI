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
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.coobird.thumbnailator.Thumbnails;

/**
 * This operator can be used to resize an image to the desired resolution be specifying
 * the width and height or scale  properties
 * in the properties.xml.
 * It uses net.coobird.thumbnailator.*  libraries for resizing the images.
 * Usage:
 * Set the width and height property before launch to resize the image to the desired resolution.
 * OR
 * Set the scale property to resize the image while maintaining the original aspect ratio.
 *
 */

public class ImageResizeOperator extends AbstractImageProcessingOperator
{
  private int width = 0;
  private int height = 0;
  public double scale = 1;

  public double getScale()
  {
    return scale;
  }

  /**
   * Set the ratio at which the image will be scaled. Maintains the original aspect ratio of the image.
   * @param scale
   */
  public void setScale(double scale)
  {
    this.scale = scale;
  }

  public int getWidth()
  {
    return width;
  }

  /**
   * Width of resized image in pixels.
   * @param width
   */
  public void setWidth(int width)
  {
    this.width = width;
  }

  public int getHeight()
  {
    return height;
  }

  /**
   * Height of resized image in pixels.
   * @param height
   */
  public void setHeight(int height)
  {
    this.height = height;
  }

  protected void resize(Data data)
  {
    try {
      bufferedImage = byteArrayToBufferedImage(data.bytesImage);
      BufferedImage resizedImage;
      if (height == width && width == 0) {
        resizedImage = Thumbnails.of(bufferedImage).scale(scale).asBufferedImage();
      } else {
        resizedImage = Thumbnails.of(bufferedImage).size(width, height).asBufferedImage();
      }
      data.bytesImage = bufferedImageToByteArray(resizedImage,data.imageType);
      output.emit(data);
    } catch (IOException e) {
      throw new RuntimeException("Error image not available to Thumbnails.of() " + e);
    }
  }

  @Override
  void processTuple(Data data)
  {
    resize(data);
  }

}
