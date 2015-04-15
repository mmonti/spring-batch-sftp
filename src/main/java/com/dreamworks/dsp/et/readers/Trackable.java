package com.dreamworks.dsp.et.readers;

import java.util.List;

/**
 * Created by mmonti on 4/15/15.
 */
public interface Trackable {

    String DEFAULT_PROCESSED_KEY = "processed";
    String DEFAULT_SUCCESS_KEY = "success";
    String DEFAULT_FAIL_KEY = "fail";

    /**
     *
     * @param key
     * @param value
     */
    void add(MultiRemoteResourceReader.TrackingType key, Object value);

}
