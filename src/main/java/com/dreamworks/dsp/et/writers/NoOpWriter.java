package com.dreamworks.dsp.et.writers;

import java.util.List;

import org.springframework.batch.item.ItemWriter;

/**
 * Created by mmonti on 4/7/15.
 */
public class NoOpWriter implements ItemWriter {

  @Override
  public void write(List items) throws Exception {

  }
}
