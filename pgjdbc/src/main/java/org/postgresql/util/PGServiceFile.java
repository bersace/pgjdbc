/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.PGProperty;

/*
 * See https://www.postgresql.org/docs/current/static/libpq-pgservice.html.
 */
public class PGServiceFile {
  private static final Logger LOGGER = Logger.getLogger(PGServiceFile.class.getName());

  public static Map load(String service) throws Exception {
    String filename = findPath();
    PGServiceFile file = new PGServiceFile();
    LOGGER.log(Level.FINE, "Reading service file " + filename);
    Scanner in = new Scanner(new FileInputStream(filename), "UTF-8");
    try {
      file.parse(in);
    } finally {
      in.close();
    }
    return file.getService(service);
  }

  private static String findPath() {
    String filename = System.getProperty("org.postgresql.pgservicefile");
    if (filename == null) {
      filename = System.getenv().get("PGSERVICEFILE");
    }
    if (filename == null) {
      filename = System.getProperty("user.home") + File.separator + ".pg_service.conf";
    }
    return filename;
  }

  private final Map<String, Map<String, String>> sections;

  public PGServiceFile() {
    sections = new HashMap();
  }

  private void parse(Scanner in) throws ParseException {
    int lineNum = 0;
    String sectionName = null;
    Map<String, String> section = null;

    while (in.hasNextLine()) {
      String line = in.nextLine();
      lineNum++;
      line = line.replaceAll("^\\s+", "");
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      } else if (line.startsWith("[")) {
        if (!line.endsWith("]")) {
          String msg = MessageFormat.format("Error in service file line {0}: missing ].", lineNum);
          throw new ParseException(msg, lineNum);
        }
        sectionName = line.substring(1, line.length() - 1);
        section = new HashMap();
        sections.put(sectionName, section);
      } else if (section == null) {
        String msg = MessageFormat.format("Error in service file line {0}: not in section.", lineNum);
        throw new ParseException(msg, lineNum);
      } else {
        String[] segment = line.split("=", 2);
        if (segment.length != 2) {
          String msg = MessageFormat.format("Error in service file line {0}: bad syntax.", lineNum);
          throw new ParseException(msg, lineNum);
        }
        section.put(segment[0].trim(), segment[1].trim());
      }
    }
  }

  public Map<String, String> getService(String name) throws SQLException {
    if (sections.containsKey(name)) {
      return sections.get(name);
    } else {
      throw new SQLException(MessageFormat.format("Unknown service {0}.", name));
    }
  }

  public static void copyProperties(Map<String, String> service, Properties urlProps) {
    if (service.containsKey("port")) {
      LOGGER.log(Level.FINEST, "Reading port from service.");
      PGProperty.PG_PORT.set(urlProps, service.get("port"));
    }
    if (service.containsKey("host")) {
      LOGGER.log(Level.FINEST, "Reading host from service.");
      PGProperty.PG_HOST.set(urlProps, service.get("host"));
    }
    if (service.containsKey("dbname")) {
      LOGGER.log(Level.FINEST, "Reading database from service.");
      PGProperty.PG_DBNAME.set(urlProps, service.get("dbname"));
    }
    for (Map.Entry<String, String> entry : service.entrySet()) {
      LOGGER.log(Level.FINEST, "Reading " + entry.getKey() + " from service.");
      urlProps.setProperty(entry.getKey(), entry.getValue());
    }
  }
}
