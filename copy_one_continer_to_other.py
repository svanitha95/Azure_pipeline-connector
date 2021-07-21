from azure.storage.blob import BlobServiceClient
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
        # print(f'Uploading {source} to {dest}')
        with open(source, 'rb') as data:
            self.client.upload_blob(name=dest, data=data, overwrite=True)            

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

# Azure
# Get this from Settings/Access keys in your Storage account on Azure portal
account_name = "bucketfordiagpolicy"
connection_string =  "DefaultEndpointsProtocol=https;AccountName=bucketfordiagpolicy;AccountKey=NJ24tR9pB9hum20o2m93phLrX8fW8i1OuETrFuOrAfxYw9+YXNilAJqOp5VihpjkN5Q4nmmGuWu2QOQXMHBZ0A==;EndpointSuffix=core.windows.net"
# source_container_name = "insights-logs-ikediagnosticlog"
target_container_name = "diagnostic-logs-container"


source_list_path = []
source_list_dir = []
activity_audit_list = []
diagnostic_logs_list = []
containers_list = []

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# to get the list of all containers in a storage account
all_containers = blob_service_client.list_containers(include_metadata=True)
for container in all_containers:
    # print(container['name'], container['metadata'])
    containers_list.append(container['name'])

if len(containers_list) == 0:
    print("list is empty")
else:            
    for i in range(0,len(containers_list)):
        matches = ["activity-logs", "auditlogs", "$logs","anitha"]
        if any(x in containers_list[i] for x in matches):
            activity_audit_list.append(containers_list[i])
        else:
            diagnostic_logs_list.append(containers_list[i])

# to list all the blobs in a source container
for h in range(0,len(diagnostic_logs_list)):
    print("=====================================================")
    print("working with ", diagnostic_logs_list[h])
    print("======================================================")
    container_client = blob_service_client.get_container_client(diagnostic_logs_list[h])
    blobs_list = container_client.list_blobs()
    for blob in blobs_list:
        source_list_path.append(blob.name)

    for i in range(0,len(source_list_path)):
        t1 = source_list_path[i].rsplit('/',1)[0]+'/'   
        source_list_dir.append(t1) 

    for path in source_list_dir:
        try:
            os.makedirs(path, exist_ok = True)
            # print("Directory '%s' created successfully" % path)
        except OSError as error:
            print("Directory '%s' can not be created" % path)    

    for path in source_list_path:
        with open(path, 'w') as file:
            file.write('content')        

    # to create a directories in a target container
    client = DirectoryClient(connection_string, target_container_name)
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++")
    for i in  range(0,len(source_list_dir)):
        source_name = source_list_dir[i].split('/')[0]
        print(source_name)
        client.upload(source_name, 'diagnostic-logs')


# # If you would like to delete the source file
# remove_blob = blob_service_client.get_blob_client(source_container_name, source_file_path)
# remove_blob.delete_blob()



