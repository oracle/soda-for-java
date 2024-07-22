/* Copyright (c) 2015, 2024, Oracle and/or its affiliates.*/

/*
   DESCRIPTION
    Pair of objects
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Max Orgiyan
 */

package oracle.json.sodautil;

public class Pair <F, S> {

  // First object in a pair
  private final F f;
 
  // Second object in a pair
  private final S s;

  public Pair(F f, S s) {
    this.f = f;
    this.s = s;
  }

  public F getFirst() {
    return f;
  }

  public S getSecond() {
    return s;
  }
}
