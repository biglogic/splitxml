import os
import sys
import json
import boto3
import botocore

from datetime import datetime
from xml.sax import parse
from xml.sax.saxutils import XMLGenerator

class CycleFile(object):

    def __init__(self, filename):
        self.basename, self.ext = os.path.splitext(filename)
        self.index = 0
        self.open_next_file()

    def open_next_file(self):
        self.index += 1
        self.file = open(self.name(), 'wb')

    def name(self):
        return '%s-s%s%s' % (self.basename,self.index , self.ext)

    def cycle(self):
        self.file.close()
        self.open_next_file()

    def write(self, str):
        self.file.write(str)

    def close(self):
        self.file.close()

class XMLBreaker(XMLGenerator):
    
    def __init__(self, break_into=None, break_after=10, out=None, *args, **kwargs):
        XMLGenerator.__init__(self, out, encoding='utf-8', *args, **kwargs)
        self.out_file = out
        self.break_into = break_into
        self.break_after = break_after
        self.context = []
        self.count = 0

    def startElement(self, name, attrs):
        XMLGenerator.startElement(self, name, attrs)
        self.context.append((name, attrs))

    def endElement(self, name):
        XMLGenerator.endElement(self, name)
        self.context.pop()
        if name == self.break_into:
            self.count += 1
            #print(name)
            if self.count == self.break_after:
                self.count = 0
                for element in reversed(self.context):
                    self.out_file.write("\n".encode('UTF-8'))
                    XMLGenerator.endElement(self, element[0])
                self.out_file.cycle()
                XMLGenerator.startDocument(self)
                for element in self.context:
                    XMLGenerator.startElement(self, *element)

def lambda_handler(event, context):

    MAX_ELEMENT = 5000
    # 16000000
    if int(event['Records'][0]['s3']['object']['size']) < 1600000:
        print("file size is less than 16MB. No need to split the file")
        return {
            'statusCode': 200,
        }

    OBJECT_LIST = event['Records'][0]['s3']['object']['key'].split('/')
    OBJECT_PATH = '/'.join(OBJECT_LIST[0:-1])
    OBJECT_NAME = OBJECT_LIST[-1]
    BUCKET = event['Records'][0]['s3']['bucket']['name']

    Time=datetime.now()
    TIMESTAMP=Time.strftime("%d-%m-%y-%H-%M-%S-%s")
    DOWNLOAD_PATH = '/tmp/download_'+TIMESTAMP
    SPLIT_FILE_PATH = '/tmp/split_file_'+TIMESTAMP
 
    os.mkdir(DOWNLOAD_PATH)
    os.mkdir(SPLIT_FILE_PATH)

    # Download file from the s3
    s3 = boto3.client('s3')
    
    try:
        s3.download_file(BUCKET, OBJECT_PATH + "/" + OBJECT_NAME, DOWNLOAD_PATH + "/" + OBJECT_NAME) 
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")

    parse(DOWNLOAD_PATH + "/" + OBJECT_NAME, XMLBreaker('customer', int(MAX_ELEMENT), out=CycleFile(SPLIT_FILE_PATH + "/" + OBJECT_NAME)))

    for filename in os.listdir(SPLIT_FILE_PATH):
        s3.upload_file(SPLIT_FILE_PATH + "/" + filename, BUCKET, OBJECT_PATH + "/" + filename)

    return {
        'statusCode': 200,
    }