/**
 * Copyright (C) 2017 David Eagen
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package net.spy.memcached.util;

/** Utility methods to read system properties and handle SecurityExceptions. */
public final class PropertyUtils {
  /** Private constructor, since this is a purely static class. */
  private PropertyUtils() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get a system property in a manner cognizant of the SecurityManager.
   *
   * @param key the name of the system property.
   * @return the string value of the system property, or null if there is no
   *     property with that key or if the property read was denied.
   * @exception NullPointerException if <code>key</code> is <code>null</code>.
   * @exception IllegalArgumentException if <code>key</code> is empty.
   */
  public static String getSystemProperty(String key) {
    String value = null;
    try {
      value = System.getProperty(key);
    } catch (SecurityException e) {
      // Do nothing, leaving the value equal to null.
    }
    return value;
  }

  /**
   * Get a system property in a manner cognizant of the SecurityManager.
   *
   * @param key the name of the system property.
   * @param defaultValue the default value to return
   * @return the string value of the system property. If there is no property
   *     with that key or if access to the property is denied by the
   *     SecurityManager, the default value is returned.
   * @exception NullPointerException if <code>key</code> is <code>null</code>.
   * @exception IllegalArgumentException if <code>key</code> is empty.
   */
  public static String getSystemProperty(String key, String defaultValue) {
    String propertyValue = PropertyUtils.getSystemProperty(key);
    return propertyValue != null ? propertyValue : defaultValue;
  }
}
