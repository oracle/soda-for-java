/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Operation result cursor. Returned by {@link OracleOperationBuilder#getCursor()}
 * method.
 */
public interface OracleCursor extends Closeable
{
  /**
   * Returns the next <code>OracleDocument</code>.
   * <p>
   * Note: any underlying exception (e.g. <code>SQLException</code> if
   * this API is implemented on top of an RDBMS), might be available in the
   * chain of causes of this <code>OracleException</code>.
   * </p>
   * <p>
   * For the Oracle RDBMS implementation of SODA, the current
   * limit for the maximum size of document that can be read is 2GB.
   * An exception will be thrown by this method if the next document's
   * size exceeds this limit.
   * </p>
   *
   * @return                            the next <code>OracleDocument</code>
   * @throws OracleException            might wrap another exception set as a
   *                                    cause
   */
  public OracleDocument next() throws OracleException;

  /**
   * Returns <code>true</code> if the next <code>OracleDocument</code>
   * is available.
   * <p>
   * Note: any underlying exception (e.g. <code>SQLException</code> if
   * this API is implemented on top of an RDBMS), might be available in the
   * chain of causes of this <code>OracleException</code>
   * </p>
   * <p>
   * For the Oracle RDBMS implementation of SODA, the current
   * limit for the maximum size of document that can be read is 2GB.
   * An exception will be thrown by this method if the next document's
   * size exceeds this limit.
   * </p>
   *
   * @return                            the next <code>OracleDocument</code>
   * @throws OracleException            might wrap another exception set as a
   *                                    cause
   */
  public boolean hasNext() throws OracleException;
}
