package com.dreamworks.dsp.et.readers;

import com.dreamworks.dsp.et.io.RemoteFileInputStreamSource;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;
import org.springframework.integration.file.remote.ClientCallback;
import org.springframework.integration.file.remote.RemoteFileTemplate;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by mmonti on 4/7/15.
 */
public class MultiRemoteResourceReader extends MultiResourceItemReader implements Trackable, StepExecutionListener {

    private Logger logger = LoggerFactory.getLogger(MultiRemoteResourceReader.class);

    private RemoteFileTemplate remoteFileTemplate;
    private String remoteFilePattern;
    private Predicate<? super ChannelSftp.LsEntry> predicate;

    private Set<String> processed;
    private Set<String> succeeded;
    private Set<String> failed;

    private String processedPromotionKey = Trackable.DEFAULT_PROCESSED_KEY;
    private String succeededPromotionKey = Trackable.DEFAULT_SUCCESS_KEY;
    private String failedPromotionKey = Trackable.DEFAULT_FAIL_KEY;

    /**
     * @param executionContext
     */
    private void doOpen(ExecutionContext executionContext) {
        logger.debug("doOpen(executionContext=[{}])", executionContext);
        super.open(executionContext);
    }

    @Override
    public void open(final ExecutionContext executionContext) throws ItemStreamException {
        logger.debug("open()");

        remoteFileTemplate.executeWithClient(new ClientCallback<ChannelSftp, Void>() {

            /**
             *
             * @param client
             * @return
             */
            @Override
            public Void doWithClient(final ChannelSftp client) {
                try {
                    final List<Resource> resources = new ArrayList();

                    // = With the pattern we retrieve the list of files in the remote end that match and we have
                    // = to transfer.
                    final String remoteFilePattern = getRemoteFilePattern();
                    if (remoteFilePattern == null || remoteFilePattern != null && remoteFilePattern.isEmpty()) {
                        throw new RuntimeException("remoteFilePattern is not valid: " + remoteFilePattern);
                    }

                    // = We need to resolve the parent folder of the remote file in order to create the full path
                    // = of the file to stream.
                    final String remoteFileParent = resolveRemoteFileParent(remoteFilePattern);
                    if (remoteFileParent == null || remoteFileParent != null && remoteFileParent.isEmpty()) {
                        throw new RuntimeException("remoteFileParent invalid. Check your remoteFilePattern: " + remoteFilePattern);
                    }

                    // = Obtain the list of files and Iterate through to create an InputStreamResource.
                    Iterable<ChannelSftp.LsEntry> remoteEntries = Iterables.consumingIterable(client.ls(remoteFilePattern));
                    if (predicate != null) {
                        // = If there's a predicate specified, use it to filter the list of files to process.
                        remoteEntries = Iterables.filter(remoteEntries, predicate);
                    }

                    for (final ChannelSftp.LsEntry currentEntry : remoteEntries) {
                        final String remoteFilePath = new StringBuilder(remoteFileParent).append(currentEntry.getFilename()).toString();
                        logger.debug("remote resource=[{}]", remoteFilePath);

                        final InputStream inputStream = client.get(remoteFilePath);

                        // = Add the resource to the list.
                        resources.add(new RemoteFileInputStreamSource(remoteFilePath, inputStream));
                    }

                    // = Setting list of resources.
                    setResources(resources.toArray(new Resource[resources.size()]));

                    doOpen(executionContext);

                } catch (SftpException e) {
                    logger.error("Error processing Remote InputStream.", e.getMessage());
                }
                return null;
            }
        });
    }

    /**
     * @param remoteFilePattern
     * @return
     */
    private String resolveRemoteFileParent(final String remoteFilePattern) {
        final Path parent = Paths.get(remoteFilePattern).getParent();
        final String separator = parent.getFileSystem().getSeparator();

        return parent.toString() + separator;
    }

    /**
     * @param remoteFileTemplate
     */
    public void setRemoteFileTemplate(RemoteFileTemplate remoteFileTemplate) {
        this.remoteFileTemplate = remoteFileTemplate;
    }

    /**
     * @param remoteFilePattern
     */
    public void setRemoteFilePattern(final String remoteFilePattern) {
        this.remoteFilePattern = remoteFilePattern;
    }

    /**
     * @return
     */
    public String getRemoteFilePattern() {
        return remoteFilePattern;
    }

    /**
     *
     * @param delegate
     */
    @Override
    public void setDelegate(ResourceAwareItemReaderItemStream delegate) {
        if (delegate instanceof TrackableAware) {
            ((TrackableAware) delegate).setTrackable(this);
        }
        super.setDelegate(delegate);
    }

    @Override
    public void add(final TrackingType key, Object value) {
        switch (key) {
            case PROCESSED  : processed.add(value.toString()); break;
            case SUCCEEDED  : succeeded.add(value.toString()); break;
            case FAILED     : failed.add(value.toString()); break;
            default:
                throw new RuntimeException("Invalid tracking type="+key);
        }
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.processed = new HashSet<>();
        this.succeeded = new HashSet<>();
        this.failed = new HashSet<>();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        // = Compute succeeded files.
        succeeded.addAll(processed);
        succeeded.removeAll(failed);

        // = Add the list to the execution context to pass it along the other steps.
        final ExecutionContext executionContext = stepExecution.getExecutionContext();
        if (executionContext != null) {
            executionContext.put(processedPromotionKey, processed);
            executionContext.put(succeededPromotionKey, succeeded);
            executionContext.put(failedPromotionKey, failed);
        }

        return ExitStatus.COMPLETED;
    }

    public void setProcessedPromotionKey(String processedPromotionKey) {
        this.processedPromotionKey = processedPromotionKey;
    }

    public void setSucceededPromotionKey(String succeededPromotionKey) {
        this.succeededPromotionKey = succeededPromotionKey;
    }

    public void setFailedPromotionKey(String failedPromotionKey) {
        this.failedPromotionKey = failedPromotionKey;
    }

    public enum TrackingType {
        SUCCEEDED,
        PROCESSED,
        FAILED
    }
}
