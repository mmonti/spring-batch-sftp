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
public class TrackingFlatFileItemReader<T> extends FlatFileItemReader<T> implements ResourceAware {

    private Logger logger = LoggerFactory.getLogger(TrackingFlatFileItemReader.class);

    private static final String DEFAULT_PROCESSED_KEY = "processed";
    private static final String DEFAULT_SUCCESS_KEY = "success";
    private static final String DEFAULT_FAIL_KEY = "fail";

    private Resource resource;
    private ExecutionContext executionContext;

    private Set<String> processed = new HashSet();
    private Set<String> succeeded = new HashSet();
    private Set<String> failed = new HashSet();

    private String processedKey = DEFAULT_PROCESSED_KEY;
    private String succeededKey = DEFAULT_SUCCESS_KEY;
    private String failedKey = DEFAULT_FAIL_KEY;

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
            processed.add(resource.getFilename());

            return result;

        } catch (Exception e) {
            logger.debug("Error reading resource=[{}], marking it as failed resource.", resource.getFilename());
            failed.add(resource.getFilename());

            return null;
        }
    }

    @Override
    protected void doClose() throws Exception {
        super.doClose();

        // = Compute succeeded files.
        succeeded.addAll(processed);
        succeeded.removeAll(failed);

        // = Add the list to the execution context to pass it along the other steps.
        executionContext.put(processedKey, processed);
        executionContext.put(succeededKey, succeeded);
        executionContext.put(failedKey, failed);
    }

    /**
     * We need to inject the step execution and use it to promote the files that were processed.
     * @param stepExecution
     */
    public void setStepExecution(final StepExecution stepExecution) {
        executionContext = stepExecution.getExecutionContext();
        logger.debug("executionContext set=[{}]", executionContext);
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

    public void setProcessedKey(String processedKey) {
        this.processedKey = processedKey;
    }

    public void setSucceededKey(String succeededKey) {
        this.succeededKey = succeededKey;
    }

    public void setFailedKey(String failedKey) {
        this.failedKey = failedKey;
    }
}
