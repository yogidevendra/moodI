package com.datatorrent.fs.s3;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.fs.s3.S3RecordReaderModule;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import com.datatorrent.api.DAG;

/**
 * Wrapper for S3RecordReaderModule and specify the below parameters as configuration to this module:
 *  - accessKey : AWS key to access S3 bucket
 *  - secretKey : AWS secret key to access S3 bucket
 *  - bucketName  : Name of S3 bucket
 *  - s3Schema  : Schema of S3 File System.
 */
public class S3RecordReaderWrapperModule extends S3RecordReaderModule
{
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;
  @NotNull
  private String bucketName;
  @NotNull
  private String s3Schema = "s3n";

  /**
   * Convert the the input files to S3URI and set it to files.
   */
  private void generateAndSetS3URIToInput()
  {
    String inputURI = s3Schema + "://" + accessKey + ":" + secretKey + "@" + bucketName + "/";
    String uriFiles = "";
    String[] inputFiles = super.getFiles().split(",");
    for (int i = 0; i < inputFiles.length; i++) {
      uriFiles += inputURI + inputFiles[i];
      if (i != inputFiles.length - 1) {
        uriFiles += ",";
      }
    }
    this.setFiles(uriFiles);
  }

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    generateAndSetS3URIToInput();
    super.populateDAG(dag, configuration);
  }

  /**
   * Return the S3 access key
   * @return accessKey
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set the S3 access key
   * @param accessKey accessKey
   */
  public void setAccessKey(String accessKey)
  {
    this.accessKey = Preconditions.checkNotNull(accessKey);
  }

  /**
   * Return the S3 secret Key
   * @return secretKey
   */
  public String getSecretKey()
  {
    return secretKey;
  }

  /**
   * Set the S3 secret Key
   * @param secretKey secretKey
   */
  public void setSecretKey(String secretKey)
  {
    this.secretKey = Preconditions.checkNotNull(secretKey);
  }

  /**
   * Return the bucket name
   * @return bucketName
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the bucket name
   * @param bucketName bucketName
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = Preconditions.checkNotNull(bucketName);
  }

  /**
   * Get the schema for s3 file system
   * @return s3Schema
   */
  public String getS3Schema()
  {
    return s3Schema;
  }

  /**
   * Set the schema for s3 file system
   * @param s3Schema s3Schema
   */
  public void setS3Schema(String s3Schema)
  {
    this.s3Schema = Preconditions.checkNotNull(s3Schema);
  }
}
