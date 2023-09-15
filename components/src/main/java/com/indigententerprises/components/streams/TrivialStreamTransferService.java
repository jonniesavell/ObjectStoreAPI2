package com.indigententerprises.components.streams;

import com.indigententerprises.services.streams.StreamTransferService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TrivialStreamTransferService implements StreamTransferService {


    @Override
    public void transferStreamData(InputStream inputStream, OutputStream outputStream) throws IOException {

        // mutable data
        int byteValue;
        while ((byteValue = inputStream.read()) != -1) {

            outputStream.write(byteValue);
        }
    }
}
