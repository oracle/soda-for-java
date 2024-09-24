/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
     Full Text Search Contains clause

   PRIVATE CLASSES
  
   NOTES
    Simple carrier of information needed to generate a JSON_TextContains clause
      - String with the contains clause
      - Not flag (should use != 'TRUE')
      - Path within row source (e.g. "$.address.location")
  
   MODIFIED    (MM/DD/YY)
    dmcmahon    08/14/15 - Creation
 */

/**
 * ContainsClause.java
 *
* Copyright (c) 2014, 2015, Oracle and/or its affiliates. All rights reserved.
 *
 * @author Doug McMahon
 */ 

package oracle.json.parser;

public class ContainsClause
{
  private final JsonQueryPath containsPath;
  private final String        searchString;
  private final boolean       notFlag;

  ContainsClause(String searchString, boolean notFlag, JsonQueryPath path)
  {
    this.containsPath = path;
    this.searchString = searchString;
    this.notFlag      = notFlag;
  }

  public JsonQueryPath getPath()
  {
    return(containsPath);
  }

  public String getSearchString()
  {
    return(searchString);
  }

  public boolean isNot()
  {
    return(notFlag);
  }
}
