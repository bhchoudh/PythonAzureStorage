from azure.storage.blob import BlockBlobService , PublicAccess
import os, os.path , time

#Enter backup path , format into a OS.path object and extract basepath name to create container with same name
fileuploadpath=input('Parent backup directory Name:- ')
os.path.normpath(fileuploadpath)         
StorageContainerName=os.path.basename(fileuploadpath)
StorageAccountName=input('Azure Storage Account Name:- ')
StorageAccountKey=input('Azure Storage Account Key:-')

#Get azure storage handle and create create first container with same name as backup directory name
block_blob_service = BlockBlobService(account_name=StorageAccountName, account_key=StorageAccountKey)
block_blob_service.create_container(StorageContainerName,public_access=PublicAccess.Blob)
print('Container Created :'+StorageContainerName)
time.sleep(5)

#Upload all blobs from a directory in local computer, check if object is a file then upload , 
#if subdirectory then create a container with same name as subdirectory and then upload files
##############################################################################################

#fileuploadpath=os.getcwd()
for file in os.listdir(fileuploadpath):
    fullfilepath=os.path.join(fileuploadpath,file)
    if os.path.isfile(fullfilepath):
        block_blob_service.create_blob_from_path(StorageContainerName,file,fullfilepath)
        print("files uploaded \t"+fullfilepath)
    else:
        #If it is a subdirectory then create a container with same name as subdirectory name 
        #and then loop through sub dir content to upload files
        subdirname=file
        fullsubdirectory=os.path.join(fileuploadpath,subdirname)
        block_blob_service.create_container(subdirname,public_access=PublicAccess.Blob)
        print('Container Created :'+subdirname)
        time.sleep(5)
        for subdirfile in os.listdir(fullsubdirectory):
            subdirfullpath=os.path.join(fullsubdirectory,subdirfile)
            if os.path.isfile(subdirfullpath):
                block_blob_service.create_blob_from_path(subdirname,subdirfile,subdirfullpath)
                print("files uploaded \t"+subdirfullpath)
    
#quit()

#browse all containers and create local directory with same name as container if not existing already 
#List all blobs in a container and download to local directory and then download blobs in each containers 
#####################################################################################################
downloadpath=input('File Download parent path:- ')
os.path.normpath(downloadpath)
containerlist=block_blob_service.list_containers()
for containers in containerlist:
    filedownloadpath=os.path.join(downloadpath,containers.name)
    if not os.path.exists(filedownloadpath):
        os.mkdir(filedownloadpath)
        print('Folder created :- '+filedownloadpath)

    BlobList = block_blob_service.list_blobs(containers.name)
    for blob in BlobList:
        filedownloadfullpath=os.path.join(filedownloadpath,blob.name)
        block_blob_service.get_blob_to_path(containers.name,blob.name,filedownloadfullpath)
        print("Blob downloaded: " + filedownloadfullpath)
    


