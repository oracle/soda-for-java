/**
 * DocumentGenerator.java
 *
* Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.
 *
 * Generator for test documents. Generates documents given a numeric
 * document number, based on a repeating pattern.
 *
 * @author   Doug McMahon
 *
 */

package oracle.json.testharness;

import oracle.soda.OracleDocument;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleException;

public class DocumentGenerator
{
    
  private static final String[] first_names =
                 {"Joseph","Robert","William","Edward","Sally",
                  "John","Linda","Samuel","Melissa","Joan","Ann"};

  private static final String[] last_names =
                 {"Smith","Jones","White","Black","Robinson"};

  private static final String[] companies =
                 {"Oracle","Microsoft","Google","IBM","SAP",
                  "Salesforce","Facebook"};

  private static final String[] titles =
                       {"Manager","Group Manager","Director","Vice President"};

  private static final String[] orgs = {"Development","Consulting","Sales"};

  private static final String[] mails = {"yahoo.com","gmail.com"};

  private static final String[] cities =
                 {"New York","Los Angeles","Boston","San Francisco",
                  "Washington","Chicago","Atlanta","Dallas","Seattle"};

  private static final String[] locations =
  {"[74.0059,40.7127]","[118.2500,34.0500]","[71.0589,42.3601]",
   "[122.4167,37.7833]","[77.0164,38.9047]","[87.6847,41.8369]",
   "[84.3900,33.7550]","[96.7970,32.7767]","[122.3331,47.6097]"};

  private static final String[] states =
                 {"NY","CA","MA","CA","DC","IL","GA","TX","WA"};

  private static final String[] streets =
                 {"1313 Mockingbird Lane",
                  "123 Main Street",
                  "1600 Pennsylvania Ave"};

  private DocumentGenerator()
  {
  }

  public static OracleDocument get(OracleDatabase jdb, int docnum)
    throws OracleException
  {
    return jdb.createDocumentFromString(get(docnum));
  }
  
  public static String get(int docnum)
  {
    
    StringBuilder sb = new StringBuilder(1000);    
    int i = docnum;

    String firstName = first_names[i % first_names.length];
    String lastName = last_names[i % last_names.length];

    long salary = 100000L + (long)i;

    salary += ((i % titles.length) * 20000L);
    salary += ((i % companies.length) * 5000L);
    salary += ((i % orgs.length) * 50000L);

    sb.setLength(0);

    sb.append("{ \"empno\" : "+(i+10000));
    sb.append(", \"name\" : \"");
    sb.append(firstName);
    sb.append(" ");
    sb.append(lastName);
    sb.append("\"");
    sb.append(", \"email\" : [\"");
    sb.append(firstName);
    sb.append("_");
    sb.append(lastName);
    sb.append("@");
    sb.append(mails[i % mails.length]);
    sb.append("\",\"");
    sb.append(firstName);
    sb.append(".");
    sb.append(lastName);
    sb.append("@icloud.com\"]");
    sb.append(", \"location\" : {\"type\" : \"Point\", \"coordinates\" : ");
    sb.append(locations[i % locations.length]);
    sb.append("}");
    sb.append(", \"address\" : {");
    sb.append("\"street\" : \"");
    sb.append(streets[i % streets.length]);
    sb.append("\"");
    sb.append(", \"city\" : \"");
    sb.append(cities[i % cities.length]);
    sb.append("\"");
    sb.append(", \"state\" : \"");
    sb.append(states[i % states.length]);
    sb.append("\"");
    sb.append("}");
    sb.append(", \"title\" : \"");
    sb.append(titles[i % titles.length]);
    sb.append("\"");
    sb.append(", \"department\" : \"");
    sb.append(orgs[i % orgs.length]);
    sb.append("\"");
    sb.append(", \"company\" : \"");
    sb.append(companies[i % companies.length]);
    sb.append("\"");

    sb.append(", \"spouse\" : null");

    sb.append(", \"salary\" : "+salary+"}");

    return(sb.toString());
  }
}
