package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.core.OConstants;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 13/11/2016.
 */
public class ONeo4jImporterMain {
  public static void main(String[] args) {

    //
    System.out.println();
    System.out.println(String.format(PROGRAM_NAME + " v.%s - %s", OConstants.getVersion(), OConstants.COPYRIGHT));
    System.out.println();
    //

    //parses the command line parameters, and starts the import (.execute). Then exits
    int returnValue = 1;
    try {

      final ONeo4jImporter neo4jImporter = ONeo4jImporterCommandLineParser.getNeo4jImporter(args);

      returnValue = neo4jImporter.execute();

    } catch (Exception ex) {
      System.err.println(ex.getMessage());
    }

    System.exit(returnValue);
    //

  }

}
