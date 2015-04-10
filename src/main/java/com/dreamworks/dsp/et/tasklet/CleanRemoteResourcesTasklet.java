package com.dreamworks.dsp.et.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.integration.file.remote.RemoteFileTemplate;
import org.springframework.integration.file.remote.SessionCallback;

import java.util.List;

/**
 * Tasklet to clean processed file using remoteFileTemplate.
 *
 * @author mauro.monti@dreamworks.com
 */
public class CleanRemoteResourcesTasklet implements Tasklet {

    private Logger logger = LoggerFactory.getLogger(CleanRemoteResourcesTasklet.class);

    private List<String> remoteResources;
    private RemoteFileTemplate remoteFileTemplate;
    private SessionCallback sessionCallback;

    @Override
    public RepeatStatus execute(final StepContribution stepContribution, final ChunkContext chunkContext) throws Exception {
        logger.debug("execute(stepContribution=[{}], chunkContext=[{}])", stepContribution, chunkContext);

        if (remoteResources == null || remoteResources.isEmpty()) {
            logger.debug("remoteResources is null or empty. No resources to clean.");
            return RepeatStatus.FINISHED;
        }

        remoteFileTemplate.execute(sessionCallback);

        return RepeatStatus.FINISHED;
    }

    public void setRemoteResources(final List<String> remoteResources) {
        this.remoteResources = remoteResources;
    }

    public void setRemoteFileTemplate(final RemoteFileTemplate remoteFileTemplate) {
        this.remoteFileTemplate = remoteFileTemplate;
    }

    public void setSessionCallback(final SessionCallback sessionCallback) {
        this.sessionCallback = sessionCallback;
    }
}
