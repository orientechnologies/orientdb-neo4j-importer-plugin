/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the parser of the command line arguments passed with the invocation of ONeo4jImporter. It contains a static method that -
 * given the arguments - returns a ONeo4jImporter object.
 *
 * @author Santo Leto
 */

public class ONeo4jImporterCommandLineParser {

  public static final String OPTION_NEO4J_DBDIR            = "neo4jdbdir";
  public static final String OPTION_ORIENTDB_PATH          = "odbdir";
  public static final String OPTION_OVERWRITE_ORIENTDB_DIR = "o";
  public static final String CREATE_INDEX_ON_NEO4JRELID    = "i";
  public List<String> mainOptions;

  static final String COMMAND_LINE_PARSER_NEO4J_DBDIR_PARAM_MANDATORY   = "Error: The Neo4j Database Directory parameter '-neo4jdbdir' is mandatory.";
  static final String COMMAND_LINE_PARSER_ORIENTDB_PATH_PARAM_MANDATORY = "Error: The OrientDB Database Directory parameter '-orientdbdir' is mandatory.";

  static final String COMMAND_LINE_PARSER_INVALID_OPTION = "Error: Invalid option '%s'";
  static final String COMMAND_LINE_PARSER_EXPECTED_VALUE = "Error: Expected value after argument '%s'";

  static final String COMMAND_LINE_PARSER_NOT_PATH            = "Error: The directory '%s' doesn't exist.";
  static final String COMMAND_LINE_PARSER_NO_WRITE_PERMISSION = "Error: You don't have write permissions on directory '%s'.";
  static final String COMMAND_LINE_PARSER_NOT_DIRECTORY       = "Error: '%s' is not a directory.";

  public ONeo4jImporterCommandLineParser() {
    this.mainOptions = new ArrayList<String>();
    this.mainOptions.add(OPTION_NEO4J_DBDIR);
    this.mainOptions.add(OPTION_ORIENTDB_PATH);
  }

  /**
   * builds a ONeo4jImporter object using the command line arguments
   *
   * @param args
   *
   * @return
   *
   * @throws Exception
   */

  public ONeo4jImporterSettings getNeo4jImporter(String[] args) throws Exception {

    final Map<String, String> options = checkOptions(readOptions(args));

    final ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbPath(options.get(OPTION_ORIENTDB_PATH));
    settings.setOverwriteOrientDbDir(options.get(OPTION_OVERWRITE_ORIENTDB_DIR) != null ?
        Boolean.parseBoolean(options.get(OPTION_OVERWRITE_ORIENTDB_DIR)) :
        false);
    settings.setCreateIndexOnNeo4jRelID(options.get(CREATE_INDEX_ON_NEO4JRELID) != null ?
        Boolean.parseBoolean(options.get(CREATE_INDEX_ON_NEO4JRELID)) :
        false);

    //checks on orientDbDir
    if (settings.getOrientDbPath() != null) {
      if (settings.getOrientDbPath().endsWith(File.separator)) {
        settings.setOrientDbPath(settings.getOrientDbPath().substring(0, settings.getOrientDbPath().length() - File.separator.length()));
      }
    }

    return settings;
  }

  private Map<String, String> checkOptions(Map<String, String> options) throws IllegalArgumentException {

    if (options.get(OPTION_NEO4J_DBDIR) == null) {
      throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_NEO4J_DBDIR_PARAM_MANDATORY));
    }

    options = setDefaultIfNotPresent(options, OPTION_OVERWRITE_ORIENTDB_DIR, "false");
    options = setDefaultIfNotPresent(options, CREATE_INDEX_ON_NEO4JRELID, "false");

    String workingDir = System.getProperty("user.dir");

    File myFile = new File(workingDir);

    String parentDir = myFile.getParent();
    String defOdbDir = myFile.getParent() + File.separator + "databases" + File.separator + "neo4j_import";
    options = setDefaultIfNotPresent(options, OPTION_ORIENTDB_PATH, defOdbDir);

    return options;
  }

  private Map<String, String> readOptions(final String[] args) throws IllegalArgumentException {

    final Map<String, String> options = new HashMap<String, String>();

    // reads arguments from command line
    for (int i = 0; i < args.length; i++) {

      // an argument cannot be shorter than one char
      if (args[i].length() < 2) {
        throw new IllegalArgumentException(String.format(COMMAND_LINE_PARSER_INVALID_OPTION, args[i]));
      }

      switch (args[i].charAt(0)) {
      case '-':
        if (args.length - 1 == i) {
          throw new IllegalArgumentException((String.format(COMMAND_LINE_PARSER_EXPECTED_VALUE, args[i])));
        }

        String option = args[i].substring(1);
        if (option.startsWith("-")) {
          option = option.substring(1);
        } else {
          if (!this.mainOptions.contains(option)) {
            throw new IllegalArgumentException((String.format(COMMAND_LINE_PARSER_INVALID_OPTION, args[i])));
          }
        }
        options.put(option, args[i + 1]);

        // jumps to the next switch
        i++;

        break;
      }
    }

    return options;
  }

  private Map<String, String> setDefaultIfNotPresent(Map<String, String> options, String option, String value)
      throws IllegalArgumentException {

    if (!options.containsKey(option)) {
      ONeo4jImporterContext.getInstance().getOutputManager().warn(String.format("WARNING: '%s' option not found. Defaulting to '%s'.", option, value));
      ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
      options.put(option, value);
    }

    return options;
  }

}
