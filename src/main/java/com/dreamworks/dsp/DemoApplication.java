package com.dreamworks.dsp;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ImportResource;

@ImportResource({"classpath:batch-config.xml"})
@SpringBootApplication
public class DemoApplication {

  public static void main(String[] args) throws JobParametersInvalidException,
      JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
    ApplicationContext context = SpringApplication.run(DemoApplication.class, args);

    JobLauncher jobLauncher = (JobLauncher) context.getBean("jobLauncher");
    Job job = (Job) context.getBean("processingJob");

    jobLauncher.run(job, new JobParameters());

    jobLauncher.run(job, new JobParameters());
  }
}
