package com.dreamworks.dsp.et.io;

import org.springframework.core.io.InputStreamResource;

import java.io.InputStream;

/**
 * InputStreamResource implementation that maps a fileName to the current InputStream.
 *
 * @author  mauro.monti@dreamworks.com
 */
public class RemoteFileInputStreamSource extends InputStreamResource {

    private String filename;

    /**
     *
     * @param filename
     * @param inputStream
     */
    public RemoteFileInputStreamSource(final String filename, final InputStream inputStream) {
        super(inputStream, filename);
        this.filename = filename;
    }

    @Override
    public String getFilename() {
        return this.filename;
    }
}
