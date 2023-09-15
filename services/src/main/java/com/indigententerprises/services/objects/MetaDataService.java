package com.indigententerprises.services.objects;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * @author jonniesavell
 */
public interface MetaDataService {

    /**
     * outputStream must be non-null and must correspond to a live stream
     *   that was truncated prior to invocation
     */
    public void serializeMetaData(
            final OutputStream outputStream,
            final Map<String, Object> attributes
    ) throws IOException;

    /**
     * inputStream must be non-null and must correspond to a live stream
     *   populated with data that this service recognizes
     */
    public Map<String, Object> deserializeMetaData(final InputStream inputStream) throws IOException;
}
