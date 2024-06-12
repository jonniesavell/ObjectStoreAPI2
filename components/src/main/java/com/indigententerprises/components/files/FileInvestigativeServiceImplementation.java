package com.indigententerprises.components.files;

import com.indigententerprises.services.files.FileInvestigativeService;

import com.indigententerprises.domain.files.FileData;
import com.indigententerprises.domain.files.FileMetaData;

import java.io.File;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public class FileInvestigativeServiceImplementation implements FileInvestigativeService {

    @Override
    public FileData investigate(final File file) throws NoSuchElementException {

        if (! file.exists()) {
            throw new NoSuchElementException("file does not exist");
        } else {
            final LinkedList<String> nameComponents = new LinkedList<>();

            {
                // mutable state
                File temp = file;

                while (temp != null) {

                    nameComponents.addFirst(temp.getName());
                    temp = temp.getParentFile();
                }
            }

            final FileMetaData fileMetaData =
                    new FileMetaData(file.getName(), nameComponents, file.length());
            final FileData fileData = new FileData(fileMetaData, file);

            return fileData;
        }
    }
}
