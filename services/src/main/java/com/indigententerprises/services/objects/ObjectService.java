package com.indigententerprises.services.objects;

import com.indigententerprises.services.common.SystemException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.NoSuchElementException;

public interface ObjectService {

    public Collection<String> retrieveKeysByPrefix(final String prefix) throws SystemException;

    public void persistObject(
            final String id,
            final int size,
            final InputStream sourceInputStream
    ) throws SystemException;

    public void retrieveObject(
            final String id,
            final OutputStream outputStream
    ) throws NoSuchElementException, SystemException;
}
