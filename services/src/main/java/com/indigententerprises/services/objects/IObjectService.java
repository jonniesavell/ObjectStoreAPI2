package com.indigententerprises.services.objects;

import com.indigententerprises.domain.objects.Handle;
import com.indigententerprises.domain.objects.HandleAndArnPair;
import com.indigententerprises.services.common.SystemException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

public interface IObjectService {

    public Map<String, Object> retrieveObjectMetaData(final Handle handle)
            throws NoSuchElementException, SystemException;

    public Collection<Handle> retrieveHandlesByPrefix(final String prefix) throws SystemException;

    public void retrieveObject(
            final Handle handle,
            final OutputStream outputStream
    ) throws NoSuchElementException, SystemException;

    public Map<String, Object> retrieveObjectAndMetaData(
            final Handle handle,
            final OutputStream outputStream
    ) throws NoSuchElementException, SystemException;

    /**
     * client accepts generated Handle
     */
    public HandleAndArnPair storeObjectAndMetaData(
            final InputStream inputStream,
            final int fileSize,
            final Map<String, Object> metadata
    ) throws SystemException;

    /**
     * client specifies Handle
     *
     * NOTE: can overwrite the contents of existing object
     */
    public HandleAndArnPair storeObjectAndMetaData(
            final InputStream inputStream,
            final Handle handle,
            final int fileSize,
            final Map<String, Object> metadata)
            throws SystemException;

    public void removeObject(final Handle handle) throws NoSuchElementException, SystemException;
}
