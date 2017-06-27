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
/**
 * Tests for basic image processing operators.
 */

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;


public class ImageProcessingTest extends ImageResizeOperator
{
  private Data data = new Data();
  File imageFile;
  private static final Logger LOG = LoggerFactory.getLogger(ImageProcessingTest.class);

  @Before
  public void setup()
  {
    String filePath;
    String[] compatibleFileTypes = {"jpg", "png", "jpeg", "fits", "gif", "tif"};
    BufferedImage bufferedImage = null;
    imageFile = new File("src/test/resources/TestImages/TestImage.jpg");
    filePath = imageFile.getAbsolutePath();
    try {
      bufferedImageType = ImageIO.read(imageFile).getType();
      bufferedImage = ImageIO.read(imageFile);
    } catch (IOException e) {
      LOG.debug("Error in writing image to byte array " + e);
    }
    for (int i = 0; i < compatibleFileTypes.length; i++) {
      if ( filePath.toString().toLowerCase().endsWith(compatibleFileTypes[i])) {
        data.imageType = compatibleFileTypes[i];
        if (data.imageType.equalsIgnoreCase("jpeg")) {
          data.imageType = "jpg";
        }
      }
    }
    data.fileName = imageFile.getName();
    data.bytesImage = bufferedImageToByteArray(bufferedImage, data.imageType);
  }

  @Test
  public void resizeTest()
  {
    ImageResizeOperator imageResizer = new ImageResizeOperator();
    imageResizer.scale = 0.5;
    BufferedImage original = byteArrayToBufferedImage(data.bytesImage);
    imageResizer.resize(data);
    BufferedImage result = byteArrayToBufferedImage(data.bytesImage);
    Assert.assertEquals((original.getWidth() * imageResizer.scale), (double)result.getWidth(), 0.0);
  }

  @Test
  public void compressTest()
  {
    File compressedFile = new File("src/test/resources/TestImages/CompressedTestImage.jpg");
    ImageCompressionOperator compress = new ImageCompressionOperator();
    compress.compressionRatio = 0.9f;
    compress.compress(data);
    try {
      FileUtils.writeByteArrayToFile(compressedFile, data.bytesImage);
    } catch (IOException e) {
      LOG.debug("Error in writing image to byte array" + e);
    }
    Assert.assertEquals(true, (imageFile.length() > compressedFile.length()));
    Assert.assertEquals(true, (compress.compressionRatio < 1f));

  }

  @Test
  public void fileFormatConversionTest()
  {
    ImageFormatConversionOperator fileFormatConverter = new ImageFormatConversionOperator();
    fileFormatConverter.toFileType = "png";
    fileFormatConverter.converter(data);
    Assert.assertEquals(true, (data.imageType.equalsIgnoreCase(fileFormatConverter.toFileType)));

  }
}
