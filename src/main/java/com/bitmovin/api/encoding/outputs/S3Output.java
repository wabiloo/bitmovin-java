package com.bitmovin.api.encoding.outputs;

/**
 * Created by
 * Ferdinand Koeppen [ferdinand.koeppen@bitmovin.com]
 * on 25.07.16.
 **/
public class S3Output extends Output
{
    private String bucketName;

    private String accessKey;

    private String secretKey;

    public S3Output()
    {
        super();
        this.setType(OutputType.S3);
    }

    public String getBucketName()
    {
        return this.bucketName;
    }

    public void setBucketName(String bucketName)
    {
        this.bucketName = bucketName;
    }

    public String getAccessKey()
    {
        return this.accessKey;
    }

    public void setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
    }

    public String getSecretKey()
    {
        return this.secretKey;
    }

    public void setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
    }
}
