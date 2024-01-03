import sys

import azure.core.exceptions as exp

print(sys.version)

import os
from azure.storage.fileshare import ShareServiceClient, ShareDirectoryClient, ShareClient, ShareFileClient

class AzureStorageAccess:
    (" This class is to manupulate Azure File Storage to create directory and upload files")

    def __init__(self,account_name,account_key,share_name,Dest_directory_name,local_folder_path,connection_string):
        self.account_name=account_name
        self.account_key=account_key
        self.share_name=share_name
        self.Dest_directory_name=Dest_directory_name
        self.local_folder_path=local_folder_path
        self.connection_string=connection_string
    # Create a ShareServiceClient


    def create_directory(self):
        try:
            # Create a ShareDirectoryClient from a connection string
            dir_client = ShareDirectoryClient.from_connection_string(
                self.connection_string, self.share_name, self.Dest_directory_name)
            # Create a ShareDirectoryClient


            # Create the directory if it doesn't exist
            if not dir_client.exists():
                print("Creating directory:", self.share_name + "/" + self.Dest_directory_name)
                dir_client.create_directory()


        except exp.ResourceExistsError as ex:
            print("ResourceExistsError:", ex.message)

    def CreateDirectory_upload_local_file(self, connection_string, local_folder_path, share_name, Dest_directory_name):
        try:

            service_client = ShareServiceClient.from_connection_string(connection_string)

            # Create a ShareClient
            share_client = service_client.get_share_client(share_name)

            # Create a ShareDirectoryClient
            directory_client = share_client.get_directory_client(Dest_directory_name)

            # Create the directory if it doesn't exist
            if not directory_client.exists():
                directory_client.create_directory()

            # Upload files from the local folder to the created directory
            for file_name in os.listdir(local_folder_path):
                file_path = os.path.join(local_folder_path, file_name)
                if os.path.isfile(file_path):
                    file_client = directory_client.get_file_client(file_name)
                    AzureFile = file_client.file_name
                    #print("AzureFile", AzureFile)
                    with open(file_path, "rb") as file:
                        #print("file_name", file_name)
                        file_client.upload_file(file)

            print("Files uploaded successfully.")
        except exp.ResourceExistsError as ex:
            print("ResourceExistsError:", ex.message)

        except exp.ResourceNotFoundError as ex:
            print("ResourceNotFoundError:", ex.message)

    def list_files_and_dirs(self, connection_string, share_name, dir_name):
        try:
            # Create a ShareClient from a connection string
            share_client = ShareClient.from_connection_string(
                connection_string, share_name)

            for item in list(share_client.list_directories_and_files(dir_name)):
                if item["is_directory"]:
                    print("Directory:", item["name"])
                else:
                    print("File:", dir_name + "/" + item["name"])

        except exp.ResourceNotFoundError as ex:
            print("ResourceNotFoundError:", ex.message)

    def download_azure_file(self, connection_string, share_name, dir_name, file_name):
        try:
            # Build the remote path
            source_file_path = dir_name + "/" + file_name

            # Add a prefix to the filename to
            # distinguish it from the uploaded file
            dest_file_name = "DOWNLOADED-" + file_name

            # Create a ShareFileClient from a connection string
            file_client = ShareFileClient.from_connection_string(
                connection_string, share_name, source_file_path)

            print("Downloading to:", dest_file_name)

            # Open a file for writing bytes on the local system
            with open(dest_file_name, "wb") as data:
                # Download the file from Azure into a stream
                stream = file_client.download_file()
                # Write the stream to the local file
                data.write(stream.readall())

        except exp.ResourceNotFoundError as ex:
            print("ResourceNotFoundError:", ex.message)

    def delete_azure_file(self, connection_string, share_name, file_path):
        try:
            # Create a ShareFileClient from a connection string
            file_client = ShareFileClient.from_connection_string(
                connection_string, share_name, file_path)

            print("Deleting file:", share_name + "/" + file_path)

            # Delete the file
            file_client.delete_file()

        except exp.ResourceNotFoundError as ex:
            print("ResourceNotFoundError:", ex.message)



if __name__ == "__main__":


    account_name = "testazurefileshareassure"
    account_key = "KlHWGZbeoK9kcQY7D0pGyibPfxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    share_name = "fileshare-storagefolder"
    Dest_directory_name = "Event102Directory"
    local_folder_path = r"C:\learn\AzureStorageTesting"
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

    #test function
    Storage=AzureStorageAccess(account_name,account_key,share_name,Dest_directory_name,local_folder_path,connection_string)
    print("call Create Directory")
    Storage.create_directory()
    print("Directory created")
    print("Upload files")
    Storage.CreateDirectory_upload_local_file(connection_string=connection_string, local_folder_path=local_folder_path,share_name=share_name, Dest_directory_name= Dest_directory_name)

    Storage.list_files_and_dirs(connection_string=connection_string,share_name=share_name,dir_name=Dest_directory_name)