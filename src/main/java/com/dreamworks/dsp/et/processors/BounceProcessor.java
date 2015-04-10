package com.dreamworks.dsp.et.processors;

import com.dreamworks.dsp.et.model.BounceModel;
import org.springframework.batch.item.ItemProcessor;

/**
 * Created by mmonti on 4/7/15.
 */
public class BounceProcessor implements ItemProcessor<BounceModel, BounceModel> {

    @Override
    public BounceModel process(BounceModel item) throws Exception {
        System.out.println("processing item="+item);
        return new BounceModel();
    }
}
