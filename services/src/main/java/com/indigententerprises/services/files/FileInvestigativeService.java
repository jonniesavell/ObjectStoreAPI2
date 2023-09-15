package com.indigententerprises.services.files;

import com.indigententerprises.domain.files.FileData;

import java.io.File;
import java.util.NoSuchElementException;

public interface FileInvestigativeService {

    public FileData investigate(File file) throws NoSuchElementException;
}
