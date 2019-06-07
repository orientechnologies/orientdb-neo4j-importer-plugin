/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterMessageHandler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.listener.OProgressMonitor;
import com.orientechnologies.orient.output.OPluginMessageHandler;

import java.util.Timer;
import java.util.TimerTask;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 13/11/2016.
 */
public class ONeo4jImporterMain {

  private static OPluginMessageHandler messageHandler = new ONeo4jImporterMessageHandler(2);

  public static int executeJob(ONeo4jImporterSettings settings) {

    String outDbUrl = settings.getOrientDbProtocol() + ":" + settings.getOrientDbPath();
    String serverInitUrl = outDbUrl.substring(0, outDbUrl.lastIndexOf('/') + 1);

    // not working inside the orientdb server context: this execution is due to a script call
    ONeo4jImporterContext.newInstance(serverInitUrl);

    ONeo4jImporterContext.getInstance().setMessageHandler(messageHandler);
    ONeo4jImporterContext.getInstance().getMessageHandler().info(ONeo4jImporterMain.class, "\n");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(ONeo4jImporterMain.class, String.format(PROGRAM_NAME + " v.%s - %s\n\n", OConstants.getVersion(), OConstants.COPYRIGHT));
    ONeo4jImporterContext.getInstance().getMessageHandler().info(ONeo4jImporterMain.class, "\n");

    //parses the command line parameters, and starts the import (.execute). Then exits
    int returnValue = 1;

    try {

      // Progress Monitor initialization
      OProgressMonitor progressMonitor = new OProgressMonitor();
      progressMonitor.initialize();

      // Timer for statistics notifying
      Timer timer = new Timer();
      try {
        timer.scheduleAtFixedRate(new TimerTask() {

          @Override
          public void run() {
            ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
          }
        }, 0, 1000);

        final ONeo4jImporter neo4jImporter = new ONeo4jImporter(settings);
        returnValue = neo4jImporter.execute();

      } finally {
        timer.cancel();
      }

    } catch (Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new RuntimeException(e);
    }
    return returnValue;
  }

}