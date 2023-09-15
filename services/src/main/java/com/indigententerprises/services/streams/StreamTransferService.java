package com.indigententerprises.services.streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StreamTransferService {

    public void transferStreamData(
            final InputStream inputStream,
            final OutputStream outputStream) throws IOException;
}
