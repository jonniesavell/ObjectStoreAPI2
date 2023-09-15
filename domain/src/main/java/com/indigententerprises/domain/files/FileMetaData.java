package com.indigententerprises.domain.files;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * this file will move to a less static design and adopt a dynamic (associative array) interface.
 *
 */
public class FileMetaData {

    private final String name;
    private final List<String> components;
    private final long size;

    public FileMetaData(final String name, final List<String> components, final long size) {

        this.name = name;
        this.components = components;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public List<String> getComponents() {
        return new ArrayList<String>(components);
    }

    public long getSize() {
        return size;
    }
}
