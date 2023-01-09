package com.azure.learning.azure.storage.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

@RestController
public class BlobStorageController {
	
	public static final String STORAGECONNECTION_STRING="BlobEndpoint=https://gkstorage2023.blob.core.windows.net/;QueueEndpoint=https://gkstorage2023.queue.core.windows.net/;FileEndpoint=https://gkstorage2023.file.core.windows.net/;TableEndpoint=https://gkstorage2023.table.core.windows.net/;SharedAccessSignature=sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-01-09T03:20:15Z&st=2023-01-08T19:20:15Z&spr=https,http&sig=T5DOxRFV1mCTrm4yqsGSM6hw8RCrALT1E5TKJZjG%2Bn4%3D";
	
	
	
	@GetMapping("/createContainer")
	public String readBlobFile() throws InvalidKeyException, URISyntaxException, StorageException {
		String messageString=null;
		CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(STORAGECONNECTION_STRING);
		CloudBlobClient  cloudBlobClient= cloudStorageAccount.createCloudBlobClient();
		CloudBlobContainer container = cloudBlobClient.getContainerReference("gkblob");
		if(container.createIfNotExists()) {
			messageString="Container created successfully..";
		}else {
			messageString="Container Already Exists";
		}
		return messageString;
	}
	
	@GetMapping("/uploadBlob/{fileName}")
	public String uploadBlob (@PathVariable String fileName ) throws URISyntaxException, StorageException, InvalidKeyException, FileNotFoundException, IOException {
		
		CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(STORAGECONNECTION_STRING);
		CloudBlobClient cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
		CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference("gkblob");
		String dirString = "C:\\data\\"+fileName;
		
		File file = new File(dirString);
		CloudBlockBlob cloudBlockBlob =  cloudBlobContainer.getBlockBlobReference(fileName);
		cloudBlockBlob.upload(new FileInputStream(file), file.length());
		return "File uploaded successfully..";
	}
	
	@GetMapping("/getAllBlobList")
	public List<String> getAllBlobList() throws InvalidKeyException, URISyntaxException, StorageException {
		CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(STORAGECONNECTION_STRING);
		CloudBlobClient cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
		CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference("gkblob");
		
		List<String> blobList = new ArrayList<>();
		for(ListBlobItem blobItem : cloudBlobContainer.listBlobs()) {
			blobList.add("Container : "+ blobItem.getContainer().getName() 
					+"Parent URI : "+blobItem.getParent().getUri()
					+"Storage URI : "+blobItem.getStorageUri().getPrimaryUri()
					+"URI : "+blobItem.getUri());
		}
		return blobList;
	}
	
	@GetMapping("/downloadAllBlob")
	public String downloadAllBlob() throws InvalidKeyException, URISyntaxException, StorageException, FileNotFoundException {
		CloudStorageAccount account = CloudStorageAccount.parse(STORAGECONNECTION_STRING);
		CloudBlobClient blobClient = account.createCloudBlobClient();
		CloudBlobContainer blobContainer = blobClient.getContainerReference("gkblob");
		
		String downloadDirString = "C:\\download_blob\\";
		for(ListBlobItem blobItem : blobContainer.listBlobs()) {
			
			if(blobItem instanceof CloudBlob) {
				CloudBlob cloudBlob = (CloudBlob)blobItem;
				cloudBlob.download( new FileOutputStream(downloadDirString+cloudBlob.getName()));
			}
		}
		return "All Blobs downloaded successfully..";
	}
	
	@DeleteMapping("/deleteBlob/{fileName}")
	public String deleteBlob(@PathVariable String fileName) throws InvalidKeyException, URISyntaxException, StorageException {
		CloudStorageAccount account = CloudStorageAccount.parse(STORAGECONNECTION_STRING);
		CloudBlobClient blobClient = account.createCloudBlobClient();
		CloudBlobContainer blobContainer = blobClient.getContainerReference("gkblob");
		String messageString=null;
		
		for(ListBlobItem blobItem : blobContainer.listBlobs()) {
			if(blobItem instanceof CloudBlob) {
				CloudBlob blob = (CloudBlob)blobItem;
				if(blob.getName().equals(fileName)) {
					blob.delete();
					messageString="File Deleted successfully..";
				}else {
					messageString="File doesn't exists..";
				}
			}
		}
		return messageString;
	}
}
