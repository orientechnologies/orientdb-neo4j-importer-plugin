package com.orientechnologies.orient.listener;

import com.orientechnologies.orient.context.ONeo4jImporterStatistics;

/**
 * Created by gabriele on 16/03/17.
 */
public interface OStatisticsListener {

  public String updateOnEvent(ONeo4jImporterStatistics counters);

}
