/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.load.pathological;

import cascading.operation.BaseOperation;
import cascading.operation.Operation;

/**
 *
 */
public abstract class BreakingOperation<T> extends BaseOperation<T> implements Operation<T>
  {

  protected long breakAfterEvents = 500L;
  protected long eventCount = 0L;
  protected long breakAfterTime = 0L;
  protected long startTime = 0L;
  protected boolean useBreakAfterTime = false;

  protected void setEventCount( long eventCount )
    {
    this.eventCount = eventCount;
    }

  protected void incrementEventCount()
    {
    eventCount += 1;
    }

  protected boolean needsToBreak()
    {
    if( useBreakAfterTime )
      return ( System.currentTimeMillis() - breakAfterTime > startTime );
    else
      return eventCount > breakAfterEvents;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      {
      return true;
      }
    if( !( object instanceof BreakingOperation ) )
      {
      return false;
      }

    BreakingOperation that = (BreakingOperation) object;

    if( breakAfterEvents != that.breakAfterEvents )
      {
      return false;
      }
    if( breakAfterTime != that.breakAfterTime )
      {
      return false;
      }
    if( useBreakAfterTime != that.useBreakAfterTime )
      {
      return false;
      }

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = (int) ( breakAfterEvents ^ ( breakAfterEvents >>> 32 ) );
    result = 31 * result + (int) ( breakAfterTime ^ ( breakAfterTime >>> 32 ) );
    result = 31 * result + ( useBreakAfterTime ? 1 : 0 );
    return result;
    }
  }
