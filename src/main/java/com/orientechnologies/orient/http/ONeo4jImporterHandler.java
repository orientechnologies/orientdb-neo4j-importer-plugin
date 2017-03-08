package com.orientechnologies.orient.http;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by gabriele on 27/02/17.
 */
public class ONeo4jImporterHandler {

  private ExecutorService pool = Executors.newFixedThreadPool(1);
  private ONeo4jImporterJob currentJob = null;

  /**
   * Executes import with configuration;
   *
   * @param cfg
   */
  public void executeImport(ODocument cfg) {

    ONeo4jImporterJob job = new ONeo4jImporterJob(cfg, new ONeo4ImporterListener() {
      @Override
      public void onEnd(ONeo4jImporterJob oTeleporterJob) {
        currentJob = null;
      }
    });

    job.validate();

    currentJob = job;
    pool.execute(job);

  }
}
