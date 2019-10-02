/**
 * (c) Copyright 2019 freiheit.com technologies GmbH
 *
 * Created on 2019-09-26 by Marco Kortkamp (marco.kortkamp@freiheit.com)
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

package net.spy.memcached;

import net.spy.memcached.ops.OperationState;

/**
 * Status, which tells if a fillWriteBuffer method has
 * successfully been completed or not.
 *
 * @author Marco Kortkamp (marco.kortkamp@freiheit.com)
 */
public enum FillWriteBufferStatus {
    SUCCESS,
    /**
     * An operation may be marked as canceled (by an async callback),
     * before a fillWriteBuffer completed successfully.
     * <p>
     * This has been observed, if a large values are written in
     * a very high frequency to the memcached and the payload contains
     * the error-code ERR_2BIG.
     * <p>
     * @see <a href="https://github.com/couchbase/spymemcached/pull/17/commits/d29fea258f6595922b19667efd39a41611d0c0ec">Bug-Report</a>
     */
    OP_STATUS_IS_COMPLETED,
    /**
     * The residual entries are just for paranoia. If the operation
     * state can switch to completed, it might switch to another state
     * as well. Although this has not been observed jet TBMK.
     */
    OP_STATUS_IS_WRITE_QUEUED,
    OP_STATUS_IS_READING,
    OP_STATUS_IS_RETRY
    ;

    public static FillWriteBufferStatus forOperationState(final OperationState opState) {
        switch (opState){
            case WRITE_QUEUED:
                return OP_STATUS_IS_WRITE_QUEUED;
            case WRITING:
                return SUCCESS;
            case READING:
                return OP_STATUS_IS_READING;
            case COMPLETE:
                return OP_STATUS_IS_COMPLETED;
            case RETRY:
                return OP_STATUS_IS_RETRY;
        }
        return null;
    }

    public boolean isSuccess() {
        return this == SUCCESS;
    }
}
