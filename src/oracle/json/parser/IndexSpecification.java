/* Copyright (c) 2014, 2017, Oracle and/or its affiliates. 
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

import oracle.soda.rdbms.impl.SODAUtils;

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

  private boolean is_parsed = false;

  private String idxName   = null;
  private String language  = null;

  private IndexColumn[] columns = new IndexColumn[0];
  private boolean is_unique = false;
  private boolean is_scalarRequired = false;
  private boolean is_lax = false;
  private String search_on = null;
  private boolean is_121_text_index_with_lang = false;
  private String dataguide = null;

  public IndexSpecification(InputStream inp)
  {
    source = inp;
  }

  private void makeAndThrowException(QueryMessage msg, Object... params)
   throws QueryException
  {
    QueryException.throwSyntaxException(msg, params);
  }

  private void close()
    throws QueryException
  {
    try
    {
      source.close();
    }
    catch (IOException e)
    {
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
    boolean hasScalarRequired = false;
    boolean hasLax = false;
    QueryException ex = null;

    try
    {
      if (source == null)
        makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

      DocumentLoader loader = new DocumentLoader(source);

      JsonObject jObj = (JsonObject)loader.parse();

      close();

      ArrayList<IndexColumn> columnList = new ArrayList<IndexColumn>();

      for (Entry<String, JsonValue> entry : jObj.entrySet()) 
      {
        String    entryKey = entry.getKey();
        JsonValue entryVal = entry.getValue();

        JsonValue.ValueType vtype = entryVal.getValueType();

        if (entryKey.equalsIgnoreCase("name"))
        {
          if (vtype != JsonValue.ValueType.STRING)
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "index name",
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
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "unique",
              "BOOLEAN", vtype.toString());

          hasUnique = true;
        }
        else if (entryKey.equalsIgnoreCase("scalarRequired"))
        {
          if (vtype == JsonValue.ValueType.TRUE)
            is_scalarRequired = true;
          else if (vtype == JsonValue.ValueType.FALSE)
            is_scalarRequired = false;
          else
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "scalarRequired",
              "BOOLEAN", vtype.toString());

          if (is_scalarRequired && is_lax)
            makeAndThrowException(QueryMessage.EX_SCALAR_AND_LAX);

          hasScalarRequired = true;
        }
        else if (entryKey.equalsIgnoreCase("lax"))
        {
          if (vtype == JsonValue.ValueType.TRUE)
            is_lax = true;
          else if (vtype == JsonValue.ValueType.FALSE)
            is_lax = false;
          else
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "lax",
              "BOOLEAN", vtype.toString());

          if (is_scalarRequired && is_lax)
            makeAndThrowException(QueryMessage.EX_SCALAR_AND_LAX);

          hasLax = true;
        }
        else if (entryKey.equalsIgnoreCase("language"))
        {
          if (vtype != JsonValue.ValueType.STRING)
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "language",
              "STRING", vtype.toString());

          language = ((JsonString)entryVal).getString();
        }
        else if (entryKey.equalsIgnoreCase("dataguide"))
        {
          if (vtype != JsonValue.ValueType.STRING)
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "dataguide",
              "STRING", vtype.toString());

          dataguide = ((JsonString)entryVal).getString();

          if (!dataguide.equalsIgnoreCase("on") &&
            !dataguide.equalsIgnoreCase("off"))
            makeAndThrowException(QueryMessage.EX_BAD_DATAGUIDE_VALUE, dataguide);
        }
        else if (entryKey.equalsIgnoreCase("search_on"))
        {
          if (vtype != JsonValue.ValueType.STRING)
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "search_on",
              "STRING", vtype.toString());

          search_on = ((JsonString)entryVal).getString();

          // Valid values are:
          //   "none"
          //   "text" (text index only)
          //   "text_value" (text and SDATA index, default)
          if (!search_on.equalsIgnoreCase("text") &&
              !search_on.equalsIgnoreCase("text_value") &&
              !search_on.equalsIgnoreCase("none"))
              makeAndThrowException(QueryMessage.EX_BAD_SEARCH_ON_VALUE, search_on);
        }
        // Note: old text index with languages is not officially supported
        // on 12.1. We are allowing it here, just to continue
        // running tests that we have written for it, in case
        // it needs to be revived. Although we can run the
        // tests, it's not usable in production for multiple reasons:
        // 12.1.0.2->12.2.0.1 and above upgrade is broken,
        // this index uses a slow auto lexer, etc.
        //
        // DO NOT USE TEXT INDEX WITH LANGUAGE SUPPORT IN PRODUCTION ON 12.1!!!
        else if (entryKey.equalsIgnoreCase("textIndex121WithLang"))
        {
          if (vtype == JsonValue.ValueType.TRUE)
            is_121_text_index_with_lang = true;
          else if (vtype == JsonValue.ValueType.FALSE)
            is_121_text_index_with_lang = false;
          else
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "textIndex121WithLang",
              "BOOLEAN", vtype.toString());
        }
        else if (entryKey.equalsIgnoreCase("fields"))
        {
          if (vtype != JsonValue.ValueType.ARRAY)
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "fields",
              "ARRAY", vtype.toString());

          JsonArray jArr = (JsonArray)entryVal;
          Iterator<JsonValue> iter = jArr.iterator();

          if (!iter.hasNext())
            makeAndThrowException(QueryMessage.EX_FIELDS_CANNOT_BE_EMPTY);

          while (iter.hasNext()) 
          {
            JsonValue arrElem = iter.next();
            if (arrElem.getValueType() != JsonValue.ValueType.OBJECT)
              makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "field",
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
                  makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                    "fields.path", "STRING", ftype.toString());

                path = ((JsonString)fval).getString();
              }
              else if (fkey.equalsIgnoreCase("datatype"))
              {
                if (ftype != JsonValue.ValueType.STRING)
                  makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                    "fields.datatype", "STRING", ftype.toString());

                dtype = ((JsonString)fval).getString();
                  System.out.println ("DTYPE IS " + dtype);
              }
              else if (fkey.equalsIgnoreCase("maxLength"))
              {
                lengthSpecified = true;

                if (ftype != JsonValue.ValueType.NUMBER)
                  makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                    "fields.maxLength", "NUMBER", ftype.toString());

                JsonNumber ival = (JsonNumber)fval;
                if (!ival.isIntegral())
                    makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
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
                    makeAndThrowException(QueryMessage.EX_WRONG_ORDER, order);
                }
                else if (ftype == JsonValue.ValueType.NUMBER)
                {
                  order = ((JsonNumber) fval).toString();
                  if (!order.equals("-1") && !(order.equals("1")))
                    makeAndThrowException(QueryMessage.EX_WRONG_ORDER, order);
                }
                else 
                  makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                    "fields.order", "STRING", ftype.toString());
              }
            }

            if (path == null)
              makeAndThrowException(QueryMessage.EX_INDEX_PROP_MISSING, "fields.path");
            
            PathParser pp = new PathParser(path);
            String[] parr = pp.splitAndSQLEscape();
            if (parr == null)
              makeAndThrowException(QueryMessage.EX_INDEX_ILLEGAL_PATH, path);

            idx.setPath(parr);
            if (dtype != null)
            {
              int sqlType = idx.setSqlType(dtype);

              if (sqlType == IndexColumn.SQLTYPE_NONE)
              {
                makeAndThrowException(QueryMessage.EX_INVALID_INDEX_DTYPE, dtype);
              }
              else if (sqlType != IndexColumn.SQLTYPE_CHAR && lengthSpecified)
                makeAndThrowException(QueryMessage.EX_LENGTH_NOT_ALLOWED, path);
            }

            if (maxLength > 0)
            {
              idx.setMaxLength(maxLength);
            }
            else if (maxLength < 0)
            {
              makeAndThrowException(QueryMessage.EX_INVALID_INDEX_DLEN,
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
          makeAndThrowException(QueryMessage.EX_LANGUAGE_NOT_EXPECTED);
        }

        columns = new IndexColumn[sz];
        columns = columnList.toArray(columns);
      }
      else if (sz == 0)
      {
        if (hasUnique)
          makeAndThrowException(QueryMessage.EX_FIELDS_EXPECTED, "unique");
        else if (hasScalarRequired)
          makeAndThrowException(QueryMessage.EX_FIELDS_EXPECTED, "scalarRequired");
        else if (hasLax)
          makeAndThrowException(QueryMessage.EX_FIELDS_EXPECTED, "lax");
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
      try
      {
        // This will only really attempt to close if an exception occurs
        close();
      }
      catch (QueryException e)
      {
        // This will be thrown after the try/catch/finally block
        // but only if this isn't already finalizing an exception case.
        ex = e;
      }
    }

    if (ex != null)
      throw ex;

    //
    // Minimal cross-validation
    //
    if (columns.length > 0)
    {
      if (language != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "fields", "language");
      if (search_on != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "fields", "search_on");
      if (dataguide != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "fields", "dataguide");
      if (is_121_text_index_with_lang)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "fields", "textIndex121WithLang");
    }
    else {

      if (search_on != null && is_121_text_index_with_lang)
      {
          makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS,
                                "search_on",
                                "textIndex121WithLang");
      }

      if (dataguide != null && is_121_text_index_with_lang)
      {
          makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS,
                                "dataguide",
                                "textIndex121WithLang");
      }
    }

    if ((nameRequired) && (idxName == null))
      makeAndThrowException(QueryMessage.EX_INDEX_PROP_MISSING, "name");

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

  public boolean isScalarRequired()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(is_scalarRequired);
  }

  public boolean isLax()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(is_lax);
  }

  public IndexColumn[] getColumns()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(columns);
  }

  public String getSearchOn()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(search_on);
  }

  public String getDataGuide()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return (dataguide);
  }

  public boolean is121TextIndexWithLang()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(is_121_text_index_with_lang);
  }

  // Old (12.1) style lexer. Desupported.
  public static String get121Lexer(String language)
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

  // New 12.2.0.1 and above lexer
  public static String getLexer(String language)
    throws QueryException
  {
    String lexer = null;

    // English is the default.
    if (language == null)
      lexer = null;
    else if (language.equalsIgnoreCase("english"))
      lexer = null;
    else if (language.equalsIgnoreCase("danish"))
      lexer = "DANISH";
    else if (language.equalsIgnoreCase("finnish"))
      lexer = "FINNISH";
    else if (language.equalsIgnoreCase("dutch"))
      lexer = "DUTCH";
    else if (language.equalsIgnoreCase("portuguese"))
      lexer = "PORTUGUESE";
    else if (language.equalsIgnoreCase("romanian"))
      lexer = "ROMANIAN";
    else if (language.equalsIgnoreCase("german"))
      lexer = "GERMAN";
    else if (language.equalsIgnoreCase("simp-chinese"))
      lexer = "SIMPLIFIED_CHINESE";
    else if (language.equalsIgnoreCase("trad-chinese"))
      lexer = "TRADITIONAL_CHINESE";
    else if (language.equalsIgnoreCase("korean"))
      lexer = "KOREAN";
    else if (language.equalsIgnoreCase("swedish"))
      lexer = "SWEDISH";
    else if (language.equalsIgnoreCase("japanese"))
      lexer = "JAPANESE";
    else if (language.equalsIgnoreCase("german-din"))
      lexer = "GERMAN_DIN";
    else if (language.equalsIgnoreCase("norwegian"))
      lexer = "NORWEGIAN";
    else if (language.equalsIgnoreCase("catalan"))
      lexer = "CATALAN";
    else if (language.equalsIgnoreCase("french"))
      lexer = "FRENCH";
    else if (language.equalsIgnoreCase("spanish"))
      lexer = "SPANISH";
    else if (language.equalsIgnoreCase("italian"))
      lexer = "ITALIAN";
    else if (language.equalsIgnoreCase("brazilian-portuguese"))
      lexer = "BRAZILIAN_PORTUGUESE";
    else if (language.equalsIgnoreCase("french-canadian"))
      lexer = "FRENCH_CANADIAN";
    else if (language.equalsIgnoreCase("latin-american-spanish"))
      lexer = "LATIN_AMERICAN_SPANISH";
    else if (language.equalsIgnoreCase("mexican-spanish"))
      lexer = "MEXICAN_SPANISH";

    /* Not supported with new lexer: arabic, nynorsk, bokmal, persian, croatian,
       serbian, slovak, slovenian, hebrew, thai,czech, polish, russian, greek,
       hungarian, turkish
     */
    else
      QueryException.throwSyntaxException(QueryMessage.EX_INVALID_INDEX_LANG,
        language);
    return(lexer);
  }

}
