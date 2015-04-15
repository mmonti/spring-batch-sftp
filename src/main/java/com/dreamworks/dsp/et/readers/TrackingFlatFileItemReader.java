package com.dreamworks.dsp.et.readers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

import java.util.HashSet;
import java.util.Set;

/**
 * FlatFileReader implementation that keeps track of which resources succeed and which ones failed.
 *
 * @author mauro.monti@dreamworks.com
 */
public class TrackingFlatFileItemReader<T> extends FlatFileItemReader<T> implements ResourceAware, TrackableAware {

    private Logger logger = LoggerFactory.getLogger(TrackingFlatFileItemReader.class);

    private Resource resource;
    private Trackable trackable;

    @Override
    protected void doOpen() throws Exception {
        logger.debug("doOpen()");
        super.doOpen();
    }

    @Override
    protected T doRead() throws Exception {
        logger.debug("doRead()");

        T result;
        try {
            result = super.doRead();

            // = Add the file to the processed resources list in order to track which file failed.
            logger.debug("Read item from resource=[{}]", resource.getFilename());
            trackable.add(MultiRemoteResourceReader.TrackingType.PROCESSED, resource.getFilename());

            return result;

        } catch (Exception e) {
            logger.debug("Error reading resource=[{}], marking it as failed resource.", resource.getFilename());
            trackable.add(MultiRemoteResourceReader.TrackingType.FAILED, resource.getFilename());

            return null;
        }
    }

    @Override
    protected void doClose() throws Exception {
        super.doClose();
    }

    /**
     * Sets the current resource to read.
     * @param resource
     */
    @Override
    public void setResource(final Resource resource) {
        super.setResource(resource);
        this.resource = resource;
        logger.debug("current resource=[{}]", resource.getFilename());
    }

    @Override
    public void setTrackable(final Trackable trackable) {
        this.trackable = trackable;
    }
}
