package com.fishblack.fastparquet.common;

import com.fishblack.fastparquet.utils.Utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class TempFile {
	
	private final File file;
	
	public TempFile(InputStream is) throws IOException {
		this();
		saveInputStream(is);
		
	}
	
	public TempFile(String prefix, String suffix, File directory) throws IOException{
		file = File.createTempFile(prefix, suffix, directory);
	}
	
	public TempFile() throws IOException{
		file = File.createTempFile("rawfile", ".tmp");
	}
	
	public TempFile(File f) {
		this.file = f;
	}
	
	public File saveInputStream(InputStream is) throws IOException {
		FileOutputStream fos = new FileOutputStream(file);
    	try {
    		Utils.copyStream(is, fos);
    	} finally {
    		Utils.close(fos);
    	}
    	return file;
    }
	
	public String getPath() {
		return this.file.getAbsolutePath();
	}
	
	public void delete() {
		if(file != null && !file.delete())
			this.file.deleteOnExit();
	}
	
	public File get() {
		return file;
	}
}
