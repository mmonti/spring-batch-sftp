package com.dreamworks.dsp.et.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.file.remote.SessionCallback;
import org.springframework.integration.file.remote.session.Session;

import java.io.IOException;
import java.util.List;

/**
 * Created by mmonti on 4/10/15.
 */
public class DeleteSessionCallback implements SessionCallback<Session, Void> {

    private static final Logger logger = LoggerFactory.getLogger(DeleteSessionCallback.class);

    private List<String> remoteResources;

    /**
     * Creates a callback that iterate through the list of resources and remove them using the current session.
     *
     * @param remoteResources
     */
    public DeleteSessionCallback(final List<String> remoteResources) {
        this.remoteResources = remoteResources;
    }

    /**
     *
     * @param session
     * @return
     * @throws IOException
     */
    @Override
    public Void doInSession(final Session session) throws IOException {
        for (final String currentResource : remoteResources) {
            if (!session.exists(currentResource)) {
                if (!session.remove(currentResource)) {
                    logger.debug("cannot remove remote resource=[{}]", currentResource);
                }
            } else {
                logger.debug("resource=[{}] does not exist in remote", currentResource);
            }
        }
        return null;
    }
}
