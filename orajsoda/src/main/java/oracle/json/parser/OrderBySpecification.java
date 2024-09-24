package oracle.json.parser;

import java.util.ArrayList;
import java.util.Map.Entry;

import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.JsonNumber;
import jakarta.json.JsonValue.ValueType;

/**
 * Describes the $orderby entry of a filter expression.
 * 
 * Note, this state and functionality was previously in FilterLoader. It has
 * been moved here so that it can be used without going through a
 * parse/serialization of the filter expression.
 */
public class OrderBySpecification 
{
  
  private ArrayList<String> orderKeys = null;
  private ArrayList<String> orderVals = null;
  
  public OrderBySpecification(JsonObject value) 
  {
    JsonValue orderby = value.get(FilterLoader.ORDERBY);
    if (orderby == null || 
        orderby.getValueType() != ValueType.OBJECT) {
      return;
    }
    // This is intended to be the same logic that FilterLoader
    // does in a streaming fashion. 
    for (Entry<String, JsonValue> entry : 
      orderby.asJsonObject().entrySet()) {
      String key = entry.getKey();
      JsonValue child = entry.getValue();
      switch (child.getValueType()) {
      case ARRAY:
      case OBJECT:
      case FALSE:
      case TRUE:
      case NULL:
        appendOrderBy(key, null); // signals a "bad" key
        break;
      case NUMBER:
        appendOrderBy(key, 
            ((JsonNumber)child).bigDecimalValue().toString());
        break;
      case STRING:
        appendOrderBy(key, ((JsonString)child).getString());
        break;
      default:
        throw new IllegalStateException(); // infeasible
      }
    }
  }
  
  public OrderBySpecification() { 
    
  }

  int getOrderCount()
  {
    if ((orderKeys == null) || (orderVals == null)) return(0);
    return(orderKeys.size());
  }

  /**
   * Get the Nth order key path.
   * Returns null if the position is out of bounds
   */
  String getOrderPath(int pos)
  {
    if (pos < 0) return(null);
    if (orderKeys == null) return(null);
    if (pos >= orderKeys.size()) return(null);
    return(orderKeys.get(pos));
  }
  
  /**
   * Get the Nth order key direction (1 or -1)
   * Returns null if the value is "bad" (true/false/null)
   */
  String getOrderDirection(int pos)
  {
    if (pos < 0) return(null);
    if (orderVals == null) return(null);
    if (pos >= orderVals.size()) return(null);
    return(orderVals.get(pos));
  }

  void appendOrderBy(String key, String val)
  {
    if (orderKeys == null) orderKeys = new ArrayList<String>();
    if (orderVals == null) orderVals = new ArrayList<String>();

    orderKeys.add(key);
    orderVals.add(val);
  }

}
