## DOWNLOAD FOLDER  FROM AWS AND SPLIT XML FILE AND UPLOAD IT IN S3 BUCKET  

- use python boto3 module to make connection with s3 bucket
----
``` 
parse(PATH/TO/FILE , XMLBreaker(ELEMENT, int(MAX_ELEMENT), out=CycleFile(PATH/TO/OUTPUT)))

```
---
- Download file from bucket 
```  
s3 = boto3.client('s3')
s3.download_file(BUCKET, OBJECT_PATH , OBJECT_NAME, DOWNLOAD_PATH )
``` 
---
- upload file to bucket 


```
 s3.upload_file(SPLIT_FILE_PATH + "/" + filename, BUCKET, OBJECT_PATH + "/" + filename)

```