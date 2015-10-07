/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Parses an index specification and holds the resulting information
    consisting of:
      Index name string
      boolean flag for uniqueness
      Language choice (for text indexes)
      Array of column paths
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 *  @author  Rahul Kadwe
 */

package oracle.json.parser;

import java.io.InputStream;
import java.io.IOException;

import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Iterator;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonNumber;
import javax.json.JsonValue;
import javax.json.JsonException;
import javax.json.stream.JsonParsingException;

public class IndexSpecification
{
  private final InputStream source;

  private boolean       is_parsed = false;

  private String        idxName   = null;
  private String        language  = null;
  private IndexColumn[] columns   = new IndexColumn[0]; // Empty list
  private boolean       is_unique = false;
  private boolean       is_singleton = false;

  public IndexSpecification(InputStream inp)
  {
    source = inp;
  }

  private void makeException(QueryMessage msg, Object... params)
   throws QueryException
  {
    QueryException.throwSyntaxException(msg, params);
  }

  private void close(boolean silent)
    throws QueryException
  {
    try
    {
      source.close();
    }
    catch (IOException e)
    {
      if (!silent)
        throw(new QueryException(QueryMessage.EX_INVALID_INDEX_SPEC.get(), e));
    }
  }

  /**
   * Parse the index specification, throwing an exception if something's wrong.
   * Returns the index name string, if any. If a name is required, pass true
   * for nameRequired and an exception will be thrown if a name isn't found.
   */
  public String parse(boolean nameRequired)
    throws QueryException
  {
    boolean hasUnique = false;
    boolean hasSingleton = false;

    try
    {
      if (source == null)
        makeException(QueryMessage.EX_INVALID_INDEX_SPEC);

      DocumentLoader loader = new DocumentLoader(source);

      JsonObject jObj = (JsonObject)loader.parse();

      close(false);

      ArrayList<IndexColumn> columnList = new ArrayList<IndexColumn>();

      for (Entry<String, JsonValue> entry : jObj.entrySet()) 
      {
        String    entryKey = entry.getKey();
        JsonValue entryVal = entry.getValue();

        JsonValue.ValueType vtype = entryVal.getValueType();

        if (entryKey.equalsIgnoreCase("name"))
        {
          if (vtype != JsonValue.ValueType.STRING)
            makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "index name",
                          "STRING", vtype.toString());

          idxName = ((JsonString)entryVal).getString();
        }
        else if (entryKey.equalsIgnoreCase("unique"))
        {
          if (vtype == JsonValue.ValueType.TRUE)
            is_unique = true;
          else if (vtype == JsonValue.ValueType.FALSE)
            is_unique = false;
          else
            makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "unique",
                          "BOOLEAN", vtype.toString());

          hasUnique = true;
        }
        else if (entryKey.equalsIgnoreCase("singleton"))
        {
          if (vtype == JsonValue.ValueType.TRUE)
            is_singleton = true;
          else if (vtype == JsonValue.ValueType.FALSE)
            is_singleton = false;
          else
            makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "singleton",
                          "BOOLEAN", vtype.toString());

          hasSingleton = true;
        }
        else if (entryKey.equalsIgnoreCase("language"))
        {
          if (vtype != JsonValue.ValueType.STRING)
            makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "language",
                          "STRING", vtype.toString());

          language = ((JsonString)entryVal).getString();
        }
        else if (entryKey.equalsIgnoreCase("fields"))
        {
          if (vtype != JsonValue.ValueType.ARRAY)
            makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "fields",
                          "ARRAY", vtype.toString());

          JsonArray jArr = (JsonArray)entryVal;
          Iterator<JsonValue> iter = jArr.iterator();

          if (!iter.hasNext())
            makeException(QueryMessage.EX_FIELDS_CANNOT_BE_EMPTY);

          while (iter.hasNext()) 
          {
            JsonValue arrElem = iter.next();
            if (arrElem.getValueType() != JsonValue.ValueType.OBJECT)
              makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "field",
                            "OBJECT", arrElem.getValueType().toString());

            JsonObject  obj  = (JsonObject)arrElem;
            IndexColumn idx  = new IndexColumn();

            String      path      = null;
            String      dtype     = null;
            String      order     = null;
            int         maxLength = 0;
            boolean     lengthSpecified = false;

            for (Entry<String, JsonValue> fieldEntry : obj.entrySet())
            {
              String    fkey = fieldEntry.getKey();
              JsonValue fval = fieldEntry.getValue();

              JsonValue.ValueType ftype = fval.getValueType();

              if (fkey.equalsIgnoreCase("path"))
              {
                if (ftype != JsonValue.ValueType.STRING)
                  makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                                "fields.path", "STRING", ftype.toString());

                path = ((JsonString)fval).getString();
              }
              else if (fkey.equalsIgnoreCase("datatype"))
              {
                if (ftype != JsonValue.ValueType.STRING)
                  makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                                "fields.datatype", "STRING", ftype.toString());

                dtype = ((JsonString)fval).getString();
              }
              else if (fkey.equalsIgnoreCase("maxLength"))
              {
                lengthSpecified = true;

                if (ftype != JsonValue.ValueType.NUMBER)
                  makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                                "fields.maxLength", "NUMBER", ftype.toString());

                JsonNumber ival = (JsonNumber)fval;
                if (!ival.isIntegral())
                    makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                                  "fields.maxLength", "integer", "decimal");

                maxLength = ival.intValue();
              }
              else if (fkey.equalsIgnoreCase("order"))
              {
                if (ftype == JsonValue.ValueType.STRING)
                {
                  order = ((JsonString) fval).getString();
                  if (!order.equalsIgnoreCase("asc") && !(order.equalsIgnoreCase("desc")) &&
                      !order.equals("1") && !(order.equals("-1")))
                    makeException(QueryMessage.EX_WRONG_ORDER, order);
                }
                else if (ftype == JsonValue.ValueType.NUMBER)
                {
                  order = ((JsonNumber) fval).toString();
                  if (!order.equals("-1") && !(order.equals("1")))
                    makeException(QueryMessage.EX_WRONG_ORDER, order);
                }
                else 
                  makeException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                                "fields.order", "STRING", ftype.toString());
              }
            }

            if (path == null)
              makeException(QueryMessage.EX_INDEX_PROP_MISSING, "fields.path");
            
            PathParser pp = new PathParser(path);
            String[] parr = pp.splitAndSQLEscape();
            if (parr == null)
              makeException(QueryMessage.EX_INDEX_ILLEGAL_PATH, path);

            idx.setPath(parr);
            if (dtype != null)
            {
              int sqlType = idx.setSqlType(dtype);

              if (sqlType == IndexColumn.SQLTYPE_NONE)
              {
                makeException(QueryMessage.EX_INVALID_INDEX_DTYPE, dtype);
              }
              else if (sqlType != IndexColumn.SQLTYPE_CHAR && lengthSpecified)
                makeException(QueryMessage.EX_LENGTH_NOT_ALLOWED, path);
            }

            if (maxLength > 0)
            {
              idx.setMaxLength(maxLength);
            }
            else if (maxLength < 0)
            {
              makeException(QueryMessage.EX_INVALID_INDEX_DLEN,
                            Integer.toString(maxLength));
            }
            idx.setOrder(order);
            columnList.add(idx);
            idx = null;
          }
        }
      }

      int sz = columnList.size();
      if (sz > 0)
      {
        if (language != null)
        {
          makeException(QueryMessage.EX_LANGUAGE_NOT_EXPECTED);
        }

        columns = new IndexColumn[sz];
        columns = columnList.toArray(columns);
      }
      else if (sz == 0)
      {
        if (hasUnique)
          makeException(QueryMessage.EX_FIELDS_EXPECTED, "unique");
        else if (hasSingleton)
          makeException(QueryMessage.EX_FIELDS_EXPECTED, "singleton");
      }
    }
    catch (IllegalArgumentException e)
    {
      throw new QueryException(QueryMessage.EX_INVALID_INDEX_SPEC.get(), e);
    }
    catch (JsonParsingException e)
    {
      throw new QueryException(QueryMessage.EX_INVALID_INDEX_SPEC.get(), e);
    }
    catch (JsonException e)
    {
      throw new QueryException(QueryMessage.EX_INVALID_INDEX_SPEC.get(), e);
    }
    finally
    {
      // This will only really attempt to close if an exception occurs
      close(true);
    }

    if ((nameRequired) && (idxName == null))
      makeException(QueryMessage.EX_INDEX_PROP_MISSING, "name");

    is_parsed = true;

    return(idxName);
  }

  /**
   * Parse the index specification, throwing an exception if something's wrong.
   * Returns the index name string, which is always required.
   */
  public String parse()
    throws QueryException
  {
    return(parse(true));
  }

  public String getName()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(idxName);
  }

  public String getLanguage()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(language);
  }

  public boolean isUnique()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(is_unique);
  }

  public boolean isSingleton()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(is_singleton);
  }

  public IndexColumn[] getColumns()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(columns);
  }

  public static String getLexer(String language)
    throws QueryException
  {
    String lexer = null;

    if (language == null) // Default is the English lexer
      lexer = "CTXSYS.JSONREST_ENGLISH_LEXER";
    else if (language.equalsIgnoreCase("english"))
      lexer = "CTXSYS.JSONREST_ENGLISH_LEXER";
    else if (language.equalsIgnoreCase("arabic"))
      lexer = "CTXSYS.JSONREST_ARABIC_LEXER";
    else if (language.equalsIgnoreCase("nynorsk"))
      lexer = "CTXSYS.JSONREST_NYNORSK_LEXER";
    else if (language.equalsIgnoreCase("bokmal"))
      lexer = "CTXSYS.JSONREST_BOKMAL_LEXER";
    else if (language.equalsIgnoreCase("persian"))
      lexer = "CTXSYS.JSONREST_PERSIAN_LEXER";
    else if (language.equalsIgnoreCase("croatian"))
      lexer = "CTXSYS.JSONREST_CROATIAN_LEXER";
    else if (language.equalsIgnoreCase("serbian"))
      lexer = "CTXSYS.JSONREST_SERBIAN_LEXER";
    else if (language.equalsIgnoreCase("danish"))
      lexer = "CTXSYS.JSONREST_DANISH_LEXER";
    else if (language.equalsIgnoreCase("slovak"))
      lexer = "CTXSYS.JSONREST_SLOVAK_LEXER";
    else if (language.equalsIgnoreCase("finnish"))
      lexer = "CTXSYS.JSONREST_FINNISH_LEXER";
    else if (language.equalsIgnoreCase("slovenian"))
      lexer = "CTXSYS.JSONREST_SLOVENIAN_LEXER";
    else if (language.equalsIgnoreCase("hebrew"))
      lexer = "CTXSYS.JSONREST_HEBREW_LEXER";
    else if (language.equalsIgnoreCase("thai"))
      lexer = "CTXSYS.JSONREST_THAI_LEXER";
    else if (language.equalsIgnoreCase("catalan"))
      lexer = "CTXSYS.JSONREST_CATALAN_LEXER";
    else if (language.equalsIgnoreCase("korean"))
      lexer = "CTXSYS.JSONREST_KOREAN_LEXER";
    else if (language.equalsIgnoreCase("czech"))
      lexer = "CTXSYS.JSONREST_CZECH_LEXER";
    else if (language.equalsIgnoreCase("polish"))
      lexer = "CTXSYS.JSONREST_POLISH_LEXER";
    else if (language.equalsIgnoreCase("dutch"))
      lexer = "CTXSYS.JSONREST_DUTCH_LEXER";
    else if (language.equalsIgnoreCase("portuguese"))
      lexer = "CTXSYS.JSONREST_PORTUGUESE_LEXER";
    else if (language.equalsIgnoreCase("romanian"))
      lexer = "CTXSYS.JSONREST_ROMANIAN_LEXER";
    else if (language.equalsIgnoreCase("french"))
      lexer = "CTXSYS.JSONREST_FRENCH_LEXER";
    else if (language.equalsIgnoreCase("russian"))
      lexer = "CTXSYS.JSONREST_RUSSIAN_LEXER";
    else if (language.equalsIgnoreCase("german"))
      lexer = "CTXSYS.JSONREST_GERMAN_LEXER";
    else if (language.equalsIgnoreCase("simp-chinese"))
      lexer = "CTXSYS.JSONREST_SCHINESE_LEXER";
    else if (language.equalsIgnoreCase("trad-chinese"))
      lexer = "CTXSYS.JSONREST_TCHINESE_LEXER";
    else if (language.equalsIgnoreCase("greek"))
      lexer = "CTXSYS.JSONREST_GREEK_LEXER";
    else if (language.equalsIgnoreCase("spanish"))
      lexer = "CTXSYS.JSONREST_SPANISH_LEXER";
    else if (language.equalsIgnoreCase("hungarian"))
      lexer = "CTXSYS.JSONREST_HUNGARIAN_LEXER";
    else if (language.equalsIgnoreCase("swedish"))
      lexer = "CTXSYS.JSONREST_SWEDISH_LEXER";
    else if (language.equalsIgnoreCase("italian"))
      lexer = "CTXSYS.JSONREST_ITALIAN_LEXER";
    else if (language.equalsIgnoreCase("japanese"))
      lexer = "CTXSYS.JSONREST_JAPANESE_LEXER";
    else if (language.equalsIgnoreCase("turkish"))
      lexer = "CTXSYS.JSONREST_TURKISH_LEXER";
    else
      QueryException.throwSyntaxException(QueryMessage.EX_INVALID_INDEX_LANG,
                                          language);
    return(lexer);
  }
}
