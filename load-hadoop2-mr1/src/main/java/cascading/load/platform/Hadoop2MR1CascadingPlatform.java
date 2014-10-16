/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.load.platform;

import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.stats.CascadingStats;
import org.apache.hadoop.mapreduce.TaskCounter;

/**
 * Implementation of CascadeLoadPlatform for hadoop2-mr1.
 */
public class Hadoop2MR1CascadingPlatform extends BaseHadoopCascadingPlatform
  {
  @Override
  public String getName()
    {
    return "hadoop2-mr1";
    }

  @Override
  public FlowConnector newFlowConnector()
    {
    return new Hadoop2MR1FlowConnector();
    }

  @Override
  public FlowConnector newFlowConnector( Map<Object, Object> properties )
    {
    return new Hadoop2MR1FlowConnector( properties );
    }

  protected String getMRFrameworkName()
    {
    return "yarn";
    }

  @Override
  public long getCPUMillis( CascadingStats cascadingStats )
    {
    return cascadingStats.getCounterValue( TaskCounter.CPU_MILLISECONDS );
    }
  }