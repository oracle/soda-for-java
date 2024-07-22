/* Copyright (c) 2014, 2022, Oracle and/or its affiliates.*/
/* All rights reserved.*/

/*
   DESCRIPTION
     QBE Spatial clause

   PRIVATE CLASSES
  
   NOTES
    Simple carrier of information needed to generate an SDO spatial clause:
      - Operation (SDO_INSIDE, SDO_ANYINTERACT, SDO_WITHIN_DISTANCE)
      - Not flag (should use != 'TRUE')
      - Path within row source (e.g. "$.address.location")
      - String version of the GeoJson reference
      - String with optional distance+units

   MODIFIED    (MM/DD/YY)
    dmcmahon    06/04/15 - Creation
 */

/**
 * SpatialClause.java
 *
* Copyright (c) 2014, 2022, Oracle and/or its affiliates. 
 *
 * @author Doug McMahon
 */ 

package oracle.json.parser;

public class SpatialClause
{
  static final String GEOMETRY_FIELD_NAME = "$geometry";
  static final String DISTANCE_FIELD_NAME = "$distance";
  static final String EXCLUDE_FIELD_NAME  = "$excludeDistance";
  static final String UNIT_FIELD_NAME     = "$unit";
  static final String SCALAR_REQ          = "$scalarRequired";
  static final String LAX                 = "$lax";
  static final String NEAR                = "$near";

  static final String SPATIAL_DEFAULT_UNIT      = "mile";
  static final String SPATIAL_DEFAULT_TOLERANCE = "0.05"; // meters

  private final JsonQueryPath spatialPath;
  private final String        spatialOperator;
  private final String        spatialReference;
  private final String        spatialDistance;
  private final boolean       notFlag;
  private final String        errorClause;

  SpatialClause(String oper, boolean notFlag, JsonQueryPath path,
                String geom, String distance, String errorClause)
  {
    this.spatialOperator  = oper;
    this.spatialPath      = path;
    this.spatialReference = geom;
    this.spatialDistance  = distance;
    this.notFlag          = notFlag;
    this.errorClause      = errorClause;
  }

  public JsonQueryPath getPath()
  {
    return(spatialPath);
  }

  public String getOperator()
  {
    return(spatialOperator);
  }

  public String getReference()
  {
    return(spatialReference);
  }

  public String getDistance()
  {
    return(spatialDistance);
  }

  public String getErrorClause()
  {
    return(errorClause);
  }

  public boolean isNot()
  {
    return(notFlag);
  }

  static String buildDistance(String dist, String excludeDist, String unit)
    throws QueryException
  {
    if (dist == null)
      QueryException.throwSyntaxException(QueryMessage.EX_SYNTAX_ERROR);

    if (unit == null)
      unit = SpatialClause.SPATIAL_DEFAULT_UNIT;

    // ### Should we append tolerance in meters?
    if (excludeDist != null)
      return("distance="+dist+" unit="+unit+" exclude_distance="+excludeDist);

    return("distance="+dist+" unit="+unit);
  }

  static String buildDistance(String dist, String unit)
    throws QueryException
  {
    return SpatialClause.buildDistance(dist, null, unit);
  }

  static String sdoOperatorFor(String op)
    throws QueryException
  {
    if (op.equals("$near"))            return "SDO_WITHIN_DISTANCE";
    else if (op.equals("$within"))     return "SDO_INSIDE";
    else if (op.equals("$intersects")) return "SDO_ANYINTERACT";

    QueryException.throwSyntaxException(QueryMessage.EX_NOT_AN_OPERATOR, op);

    return null;
  }
}
