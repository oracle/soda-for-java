/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

/**
 * Represents an exception thrown during a batch
 * write operation.
 *
 * @see OracleCollection#insert(Iterator)
 * @see OracleCollection#insertAndGet(Iterator)
 */
public class OracleBatchException extends OracleException
{
  private static final long serialVersionUID = 1L;

  private int processedCount;

 /**
  * Constructs an <code>OracleBatchException</code> object with a given message.
  *
  * @param message                  the description of the error.
  *                                 <code>null</code>
  *                                 if message string is non-existent
  */
  public OracleBatchException(String message)
  {
    super(message);
  }

  /**
   * Constructs an <code>OracleBatchException</code> object with a given message.
   *
   * @param message                  the description of the error.
   *                                 <code>null</code>
   *                                 if message string is non-existent
   * @param processedCount           number of operations processed
   *                                 before the error occurred
   */
  public OracleBatchException(String message, int processedCount)
  {
    super(message);
    this.processedCount = processedCount;
  }

  /**
   * Constructs an <code>OracleBatchException</code> with a given message
   * and an error code.
   *
   * @param message                  the description of the error. <code>null</code> indicates
   *                                 that the message string is non-existant
   * @param errorCode                the error code
   * @param processedCount           number of operations processed
   *                                 before the error occurred
   */
  public OracleBatchException(String message, int errorCode, int processedCount)
  {
    this(message, errorCode);
    this.processedCount = processedCount;
  }

  /**
   * Constructs an <code>OracleBatchException</code> object with a given cause.
   *
   * @param cause                   the cause of the error. <code>null</code> if
   *                                non-existent
   */
  public OracleBatchException(Throwable cause)
  {
    super(cause);
  }

  /**
   * Constructs an <code>OracleBatchException</code> object with a given cause.
   *
   * @param cause                   the cause of the error. <code>null</code> if
   *                                non-existent
   * @param processedCount          number of operations processed
   *                                before the error occurred
   */
  public OracleBatchException(Throwable cause, int processedCount)
  {
    super(cause);
    this.processedCount = processedCount;
  }

  /**
   * Number of operations processed before the error occurred.
   *
   * @return                        number of operations processed
   *                                before the error occurred
   */
  public int getProcessedCount()
  {
    return processedCount;
  }
}
