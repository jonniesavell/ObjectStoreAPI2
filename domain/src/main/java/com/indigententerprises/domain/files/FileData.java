package com.indigententerprises.domain.files;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileData {

    private final FileMetaData fileMetaData;
    private final File file;

    public FileData(final FileMetaData fileMetaData, final File file) {

        this.fileMetaData = fileMetaData;
        this.file = file;
    }

    public FileMetaData getFileMetaData() {
        return fileMetaData;
    }

    public InputStream getInputStream() throws IOException {

        return new FileInputStream(file);
    }
}
