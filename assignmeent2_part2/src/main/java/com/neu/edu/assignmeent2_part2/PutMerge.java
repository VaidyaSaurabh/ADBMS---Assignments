package com.neu.edu.assignmeent2_part2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMerge {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration(); 
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		Path inputDir = new Path(args[0]);
		Path hdfsFile = new Path(args[1]);
		FSDataInputStream in = null;
		FSDataOutputStream out = null;
		try {
			FileStatus[] inputFiles = local.listStatus(inputDir);
			out = hdfs.create(hdfsFile);
			for (int i = 0; i < inputFiles.length; i++) {
				in = local.open(inputFiles[i].getPath());
				byte[] buffer = new byte[1024];
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
		} finally {
			if (in != null)
				in.close();
			if (out != null)
				out.close();
		}
	}
}
