/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

/**
 * A general exception thrown by various methods of this API.
 * It can have other exceptions as causes (for example, in case of
 * an RDBMS implementation, an <code>OracleException</code> could
 * have a <code>SQLException</code> as a cause).
 * <p>
 * <b>
 * Note: cause exceptions are subject to change in the future. Your 
 * application code should avoid relying on them.
 * </b>
 * <p>
 * Certain API operations might provide multiple exceptions. For example,
 * for the Oracle RDBMS implementation of SODA, insert statements might
 * generate multiple SQL exceptions wrapped in SODA exceptions. In
 * these cases, the additional exceptions are accessible via
 * {@link #getNextException()}. The exception returned by
 * {@link #getNextException()} might, in turn, have another exception
 * chained to it. Thus, in order to access all exceptions in the chain,
 * an application can recursively invoke {@link #getNextException()}
 * until a <code>null</code> value is returned.
 */
public class OracleException extends Exception
{
  private static final long serialVersionUID = 1L;

  public static final int UNKNOWN_ERROR_CODE = -1;

  private final int errorCode;

  OracleException nextException;

  /**
   * Constructs an <code>OracleException</code> object with a given message.
   *
   * @param message     the description of the error. <code>null</code> indicates
   *                    that the message string is non-existant
   */
  public OracleException(String message)
  {
    this(message, UNKNOWN_ERROR_CODE);
  }

  /**
   * Constructs an <code>OracleException</code> with a specified
   * cause (for example, a <code>SQLException</code> or an
   * <code>IOException</code>).
   *
   * @param cause        the cause
   */
  public OracleException(Throwable cause)
  {
    this(cause, UNKNOWN_ERROR_CODE);
  }

 /**
  * Constructs an <code>OracleException</code> with a specified
  * cause (for example, a <code>SQLException</code> or an
  * <code>IOException</code>).
  *
  * @param cause        the cause
  * @param errorCode    integer error code
  */
  public OracleException(Throwable cause, int errorCode)
  {
    this(null, cause, errorCode);
  }

  /**
   * Constructs an <code>OracleException</code> with a given message
   * and an error code.
   *
   * @param message     the description of the error. <code>null</code> indicates
   *                    that the message string is non-existant
   * @param errorCode   the error code
   */
  public OracleException(String message, int errorCode)
  {
    this(message, null, errorCode);
  }

  /**
   * Constructs an <code>OracleException</code> with a given message,
   * cause, error code, and error prefix.
   *
   * @param message     the description of the error. <code>null</code> indicates
   *                    that the message string is non-existant
   * @param cause       the cause
   * @param errorCode   the error code
   */
  public OracleException(String message, Throwable cause, int errorCode)
  {
    super(message, cause);

    this.errorCode = errorCode;
  }
  /**
   * Returns the error code associated with this exception.
   * If the exception wraps a <code>SQLException</code>, the 
   * SQL error code is returned. Otherwise the error code
   * provided during construction is returned. If no error 
   * code was provided during construction either, 
   * <code>OracleException.UNKNOWN_ERROR_CODE</code> is returned.
   *
   * @return           the error code
   */
  public int getErrorCode()
  {
    return(errorCode);
  }

  /**
   * Returns the next OracleException in the chain or <code>null</code>
   * if none.
   *
   * @return                       the next exception, <code>null</code>
   *                               if none
   */
  public OracleException getNextException()
  {
    return nextException;
  }

  /**
   * Adds an <code>OracleException</code> to the chain of exceptions.
   *
   * @param nextException the exception
   */
  public void setNextException(OracleException nextException)
  {
    this.nextException = nextException;
  }
}
