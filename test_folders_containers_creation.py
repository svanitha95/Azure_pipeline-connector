from azure.storage.blob import BlobServiceClient
from azure.storage.fileshare import ShareServiceClient
from azure.storage.blob import ContainerClient
import os

class DirectoryClient:
    def __init__(self, connection_string, container_name):
        service_client = BlobServiceClient.from_connection_string(connection_string)
        self.client = service_client.get_container_client(container_name)
 

    def upload(self, source, dest):
        '''
        Upload a file or directory to a path inside the container
        '''
        if (os.path.isdir(source)):
            self.upload_dir(source, dest)
        else:
            self.upload_file(source, dest)

    def upload_file(self, source, dest):
        '''
        Upload a single file to a path inside the container
        '''
        print(f'Uploading {source} to {dest}')
        with open(source, 'rb') as data:
            self.client.upload_blob(name=dest, data=data)            

    def upload_dir(self, source, dest):
        '''
        Upload a directory to a path inside the container
        '''
        prefix = '' if dest == '' else dest + '/'
        prefix += os.path.basename(source) + '/'
        for root, dirs, files in os.walk(source):
            for name in files:
                dir_part = os.path.relpath(root, source)
                dir_part = '' if dir_part == '.' else dir_part + '/'
                file_path = os.path.join(root, name)
                blob_path = prefix + dir_part + name
                self.upload_file(file_path, blob_path)            

SAMPLE_DIRS = [
  'cats/calico',
  'cats/siamese',
  'cats/tabby'
]

SAMPLE_FILES = [
  'readme.txt',
  'cats/herds.txt',
  'cats/calico/anna.txt',
  'cats/calico/felix.txt',
  'cats/siamese/mocha.txt',
  'cats/tabby/bojangles.txt'
]

for path in SAMPLE_DIRS:
    try:
        os.makedirs(path, exist_ok = True)
        print("Directory '%s' created successfully" % path)
    except OSError as error:
        print("Directory '%s' can not be created" % path)

for path in SAMPLE_FILES:
    with open(path, 'w') as file:
        file.write('content') 

CONTAINER_NAME = "insights-activity-logs"
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=bucketfordiagpolicy;AccountKey=NJ24tR9pB9hum20o2m93phLrX8fW8i1OuETrFuOrAfxYw9+YXNilAJqOp5VihpjkN5Q4nmmGuWu2QOQXMHBZ0A==;EndpointSuffix=core.windows.net"

client = DirectoryClient(CONNECTION_STRING, CONTAINER_NAME)
print("++++++++++++++++++++++++++++++++++++++++++++++++++++")
client.upload('cats', 'cat-herding-test12')

