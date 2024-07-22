/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

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
 *  @author  Maxim Orgiyan
 */

package oracle.json.parser;

import java.io.InputStream;
import java.io.IOException;

import java.math.BigDecimal;

import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Iterator;

import jakarta.json.stream.JsonParser;
import jakarta.json.stream.JsonParserFactory;
import jakarta.json.JsonException;
import jakarta.json.stream.JsonParser.Event;

import oracle.json.sodacommon.JsonFactoryProvider;

public class IndexSpecification
{
  private final InputStream       source;
  private final JsonParserFactory factory;

  private boolean       is_parsed = false;

  private String        idxName = null;
  private String        language = null;
  private IndexColumn[] columns = new IndexColumn[0]; // Empty list
  private boolean       is_unique = false;
  private boolean       is_scalarRequired = false;
  private boolean       is_lax = false;
  private JsonQueryPath spatial = null;
  private String        search_on = null;
  private boolean       is_121_text_index_with_lang = false;
  private String        dataguide = null;
  private boolean       indexNulls = false;
  private boolean       force = false;
  private BigDecimal    ttl = null;   // TTL in #seconds
  private boolean       multivalue = false;

  public IndexSpecification(JsonFactoryProvider jProvider, InputStream inp)
  {
    factory = jProvider.getParserFactory();
    this.source = inp;
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

  private String eventToString(Event ev)
  {
    switch (ev)
    {
    case START_OBJECT:
    case END_OBJECT:     return "OBJECT";
    case START_ARRAY:
    case END_ARRAY:      return "ARRAY";
    case VALUE_TRUE:     return "TRUE";
    case VALUE_FALSE:    return "FALSE";
    case VALUE_NULL:     return "NULL";
    case VALUE_STRING:   return "STRING";
    case VALUE_NUMBER:   return "NUMBER";
    case KEY_NAME:       return "KEY";
    default:
      break;
    }
    return ev.toString();
  }

  private String getString(JsonParser jParser, String keyName)
    throws QueryException
  {
    if (!jParser.hasNext())
      makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

    Event ev = jParser.next();
    if (ev != Event.VALUE_STRING)
      makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                            keyName, "STRING", eventToString(ev));
    return jParser.getString();
  }

  private BigDecimal getNumber(JsonParser jParser, String keyName)
    throws QueryException
  {
    if (!jParser.hasNext())
      makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

    Event ev = jParser.next();
    if (ev != Event.VALUE_NUMBER)
      makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                            keyName, "NUMBER", eventToString(ev));
    return jParser.getBigDecimal();
  }

  private boolean getBoolean(JsonParser jParser, String keyName)
    throws QueryException
  {
    boolean result = false;

    if (!jParser.hasNext())
      makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

    Event ev = jParser.next();
    switch (ev)
    {
    case VALUE_TRUE:
      result = true;
      break;
    case VALUE_FALSE:
      result = false;
      break;
    default:
      makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                            keyName, "BOOLEAN", eventToString(ev));
    }

    return result;
  }

  private void skipField(JsonParser jParser)
    throws QueryException
  {
    int depth = 0;
    do
    {
      if (!jParser.hasNext())
        makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

      Event ev = jParser.next();
      switch (ev)
      {
      case START_OBJECT:
      case START_ARRAY:
        ++depth;
        break;
      case END_OBJECT:
      case END_ARRAY:
        --depth;
        break;
      default:
        break;
      }
    } while (depth > 0);
  }

  private ArrayList<IndexColumn> processFields(JsonParser jParser)
    throws QueryException
  {
    int num = 0;

    ArrayList<IndexColumn> columnList = new ArrayList<IndexColumn>();

    if (!jParser.hasNext())
      makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

    Event ev = jParser.next();
    if (ev != Event.START_ARRAY)
      makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "fields",
                            "ARRAY", eventToString(ev));

    boolean endFields = false;
    boolean inField = false;

    String      path      = null;
    String      dtype     = null;
    String      order     = null;
    int         maxLength = 0;
    boolean     lengthSpecified = false;

    while (jParser.hasNext())
    {
      ev = jParser.next();

      switch (ev)
      {
      case END_ARRAY:
        endFields = true;
        break;

      case START_OBJECT:
        if (inField)
          makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

        inField = true;

        /* Reset the field properties */
        path            = null;
        dtype           = null;
        order           = null;
        maxLength       = 0;
        lengthSpecified = false;

        break;

      case END_OBJECT:
        if (!inField)
          makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

        inField = false;

        if (path == null)
          makeAndThrowException(QueryMessage.EX_INDEX_PROP_MISSING, "fields.path");

        IndexColumn idx = new IndexColumn();

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
        ++num;

        break;

      case KEY_NAME:
        if (!inField)
          makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

        String fkey = jParser.getString();

        if (fkey.equalsIgnoreCase("path"))
          path = getString(jParser, "fields.path");
        else if (fkey.equalsIgnoreCase("datatype"))
          dtype = getString(jParser, "fields.datatype");
        else if (fkey.equalsIgnoreCase("order"))
        {
          ev = jParser.next();

          switch (ev)
          {
          case VALUE_STRING:
            order = jParser.getString();
            if (!order.equalsIgnoreCase("asc") && !(order.equalsIgnoreCase("desc")) &&
                !order.equals("1") && !(order.equals("-1")))
              makeAndThrowException(QueryMessage.EX_WRONG_ORDER, order);
            break;

          case VALUE_NUMBER:
            order = jParser.getBigDecimal().toString();
            if (!order.equals("-1") && !(order.equals("1")))
              makeAndThrowException(QueryMessage.EX_WRONG_ORDER, order);
            break;

          default:
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                                  "fields.order", "STRING", eventToString(ev));
          }
        }
        else if (fkey.equalsIgnoreCase("maxLength"))
        {
          lengthSpecified = true;
          BigDecimal ival = getNumber(jParser, "fields.maxLength");
          try
          {
            maxLength = ival.intValueExact();
          }
          catch (ArithmeticException e)
          {
            makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE,
                                  "fields.maxLength", "integer", "decimal");
          }
        }
        else
        {
          // Skip the next field
          skipField(jParser);
        }

        break;

      case START_ARRAY:
      default:
        makeAndThrowException(QueryMessage.EX_INDEX_PROP_WRONG_TYPE, "fields",
                              "OBJECT", eventToString(ev));
        break;
      }

      if (endFields) break;
    }

    if (inField)
      makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

    if (num == 0)
      makeAndThrowException(QueryMessage.EX_FIELDS_CANNOT_BE_EMPTY);

    return columnList;
  }

  /**
   * Parse the index specification, throwing an exception if something's wrong.
   * Returns the index name string, if any. If a name is required, pass true
   * for nameRequired and an exception will be thrown if a name isn't found.
   */
  public String parse(boolean nameRequired, boolean dropSpec)
    throws QueryException
  {
    JsonParser jParser = null;
    boolean hasUnique = false;
    boolean hasScalarRequired = false;
    boolean hasLax = false;
    boolean hasIndexNULLS = false;
    boolean hasMultivalue = false;
    QueryException ex = null;

    try
    {
      if (source == null)
        makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

      jParser = factory.createParser(source);

      ArrayList<IndexColumn> columnList = null;

      while (jParser.hasNext())
      {
        Event ev = jParser.next();

        switch (ev)
        {
        case START_OBJECT:
        case END_OBJECT:
          break;

        case START_ARRAY:
        case END_ARRAY:
          makeAndThrowException(QueryMessage.EX_INVALID_INDEX_SPEC);

        case KEY_NAME:
          String entryKey = jParser.getString();
          if (entryKey.equalsIgnoreCase("name"))
            idxName = getString(jParser, "index name");
          else if (entryKey.equalsIgnoreCase("unique"))
          {
            is_unique = getBoolean(jParser, "unique");
            hasUnique = true;
          }
          else if (entryKey.equalsIgnoreCase("scalarRequired"))
          {
            is_scalarRequired = getBoolean(jParser, "scalarRequired");
            if (is_lax && is_scalarRequired)
              makeAndThrowException(QueryMessage.EX_SCALAR_AND_LAX);
            hasScalarRequired = true;
          }
          else if (entryKey.equalsIgnoreCase("lax"))
          {
            is_lax = getBoolean(jParser, "lax");
            if (is_lax && is_scalarRequired)
              makeAndThrowException(QueryMessage.EX_SCALAR_AND_LAX);
            hasLax = true;
          }
          else if (entryKey.equalsIgnoreCase("indexNulls"))
          {
            indexNulls = getBoolean(jParser, "indexNulls");
            hasIndexNULLS = true;
          }
          else if (entryKey.equalsIgnoreCase("force") && dropSpec)
          {
            // This is only for internal use from SODA REST, since
            // the latter requires an index spec for dropping the index.
            // This provides a way to set the force option when dropping
            // the index.
            force = getBoolean(jParser, "force");
          }
          else if (entryKey.equalsIgnoreCase("spatial"))
            spatial = new JsonQueryPath(getString(jParser, "spatial"));
          else if (entryKey.equalsIgnoreCase("language"))
            language = getString(jParser, "language");
          else if (entryKey.equalsIgnoreCase("dataguide"))
          {
            dataguide = getString(jParser, "dataguide");
            if (!dataguide.equalsIgnoreCase("on") &&
                !dataguide.equalsIgnoreCase("off"))
              makeAndThrowException(QueryMessage.EX_BAD_DATAGUIDE_VALUE,
                                    dataguide);
          }
          else if (entryKey.equalsIgnoreCase("search_on"))
          {
            search_on = getString(jParser, "search_on");

            // Valid values are:
            //   "none"
            //   "text" (text index only)
            //   "text_value" (text and SDATA index, default)
            if (!search_on.equalsIgnoreCase("text") &&
                !search_on.equalsIgnoreCase("text_value") &&
                !search_on.equalsIgnoreCase("none"))
              makeAndThrowException(QueryMessage.EX_BAD_SEARCH_ON_VALUE,
                                    search_on);
          }
          else if (entryKey.equalsIgnoreCase("textIndex121WithLang"))
          {
            // Setting this flag (textIndex121WithLang) to true
            // will allow old text index with languages to be specified on 12.1.
            //
            // But note that old text index with languages is not officially supported
            // on 12.1. We are allowing it here with this flag as an extra precaution,
            // just to continue running tests that we have written for it, in case
            // it needs to be revived. Although we can run the
            // tests, it's not usable in production for two reasons:
            //
            // (1) 12.1.0.2->12.2.0.1 and above upgrade is broken,
            // because preferences used by the lexer for languages are missing
            // on 12.2.0.1 and above. 
            //
            // (2) The index uses a slow auto lexer for languages
            //
            // DO NOT USE TEXT INDEX WITH LANGUAGES IN PRODUCTION ON 12.1!!!
            is_121_text_index_with_lang = getBoolean(jParser, "textIndex121WithLang");
          }
          else if (entryKey.equalsIgnoreCase("fields"))
          {
            columnList = processFields(jParser);
          }
          else if (entryKey.equalsIgnoreCase("ttl")) {
            ttl = getNumber(jParser, "ttl");
          }
          else if (entryKey.equalsIgnoreCase("multivalue")) {
            multivalue = getBoolean(jParser, "multivalue");
            hasMultivalue = true;
          }
          else
          {
            //Skip unwanted field value
            skipField(jParser);
          }
        }
      }

      int sz = (columnList == null) ? 0 : columnList.size();

      if (sz > 0)
      {
        if (language != null)
          makeAndThrowException(QueryMessage.EX_LANGUAGE_NOT_EXPECTED);

        columns = new IndexColumn[sz];
        columns = columnList.toArray(columns);
      }
      else if (sz == 0)
      {
        if (hasUnique)
          makeAndThrowException(QueryMessage.EX_FIELDS_REQUIRED,
                                "unique");

        if (hasIndexNULLS)
          makeAndThrowException(QueryMessage.EX_FIELDS_REQUIRED,
                                "indexNulls");

        if (hasMultivalue)
          makeAndThrowException(QueryMessage.EX_FIELDS_REQUIRED,
                                "multivalue");

        if (spatial == null)
        {
          if (hasScalarRequired)
            makeAndThrowException(QueryMessage.EX_FIELDS_OR_SPATIAL_REQUIRED,
                                  "scalarRequired");
          else if (hasLax)
            makeAndThrowException(QueryMessage.EX_FIELDS_OR_SPATIAL_REQUIRED,
                                  "lax");
        }
      }
    }
    catch (IllegalArgumentException e)
    {
      throw new QueryException(QueryMessage.EX_INVALID_INDEX_SPEC.get(), e);
    }
    catch (JsonException e)
    {
      throw new QueryException(QueryMessage.EX_INVALID_INDEX_SPEC.get(), e);
    }
    finally
    {
      // Exceptions from closing will be thrown after the try/catch/finally
      // block but only if this isn't already finalizing an exception case.
      // This is done by setting variable ex.
      try
      {
        if (jParser != null) jParser.close();
        close();
      }
      catch (JsonException e)
      {
        ex = new QueryException(QueryMessage.EX_INVALID_INDEX_SPEC.get(), e);
      }
      catch (QueryException e)
      {
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
      if (spatial != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "fields", "spatial");
      if (language != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "fields", "language");
      if (search_on != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "fields", "search_on");
      if (dataguide != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "fields", "dataguide");
      if (is_121_text_index_with_lang)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "fields", "textIndex121WithLang");
    }
    else if (spatial != null)
    {
      if (language != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "spatial", "language");
      if (search_on != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "spatial", "search_on");
      if (dataguide != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "spatial", "dataguide");
      if (is_121_text_index_with_lang)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "spatial", "textIndex121WithLang");
      if (ttl != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "spatial", "ttl");
      if (hasMultivalue)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "spatial", "multivalue");

    }
    else if (search_on != null)
    {
      if (is_121_text_index_with_lang)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "search_on", "textIndex121WithLang");
      if (ttl != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "search_on", "ttl");
      if (hasMultivalue)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "search_on", "multivalue");
    }
    else if (dataguide != null)
    {
      if (is_121_text_index_with_lang)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "dataguide", "textIndex121WithLang");
      if (ttl != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "dataguide", "ttl");
      if (hasMultivalue)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "dataguide", "multivalue");
    }

    if (hasMultivalue)
    {
      if (is_121_text_index_with_lang)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "multivalue", "textIndex121WithLang");
      if (ttl != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "multivalue", "ttl");
      if (language != null)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "multivalue", "language");
      if (hasLax)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "multivalue", "lax");
      if (hasScalarRequired)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "multivalue", "scalarRequired");
      if (hasIndexNULLS)
        makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "multivalue", "indexNulls");

      for (IndexColumn column : columns)
      {
        String sqlTypeName = column.getSqlTypeName();
        int maxLength = column.getMaxLength();

        if (sqlTypeName != null)
          makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "datatype", "multivalue");

        if (maxLength != 0)
          makeAndThrowException(QueryMessage.EX_INCOMPATIBLE_FIELDS, "maxlength", "multivalue");
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
    return(parse(true, false));
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

  public JsonQueryPath getSpatialPath()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(spatial);
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

  public BigDecimal getTTL() {
    return ttl;
  }

  public boolean getMultiValue() {
    return multivalue;
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

  public boolean indexNulls()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(indexNulls);
  }

  public boolean force()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(force);
  }

  public IndexColumn[] getColumns()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(columns);
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
    else if (language.equalsIgnoreCase("simplified_chinese"))
      lexer = "SIMPLIFIED_CHINESE";
    else if (language.equalsIgnoreCase("traditional_chinese"))
      lexer = "TRADITIONAL_CHINESE";
    else if (language.equalsIgnoreCase("korean"))
      lexer = "KOREAN";
    else if (language.equalsIgnoreCase("swedish"))
      lexer = "SWEDISH";
    else if (language.equalsIgnoreCase("japanese"))
      lexer = "JAPANESE";
    else if (language.equalsIgnoreCase("german_din"))
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
    else if (language.equalsIgnoreCase("brazilian_portuguese"))
      lexer = "BRAZILIAN_PORTUGUESE";
    else if (language.equalsIgnoreCase("french_canadian"))
      lexer = "FRENCH_CANADIAN";
    else if (language.equalsIgnoreCase("latin_american_spanish"))
      lexer = "LATIN_AMERICAN_SPANISH";
    else if (language.equalsIgnoreCase("mexican_spanish"))
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
