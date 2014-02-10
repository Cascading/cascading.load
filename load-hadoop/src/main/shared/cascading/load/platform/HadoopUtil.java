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

import java.io.IOException;

import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * Utility class to fetch meta information about the current hadoop environment.
 */
public class HadoopUtil
  {

  public static int getNumTaskTrackers( JobConf jobConf )
    {
    try
      {
      JobClient jobClient = new JobClient( jobConf );
      ClusterStatus status = jobClient.getClusterStatus();

      return status.getTaskTrackers();
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "failed accessing hadoop cluster", exception );
      }
    }

  public static int getMaxConcurrentMappers()
    {
    JobConf jobConf = new JobConf();

    return getNumTaskTrackers( jobConf ) * jobConf.getInt( "mapred.tasktracker.map.tasks.maximum", 2 );
    }

  public static int getMaxConcurrentReducers()
    {
    JobConf jobConf = new JobConf();
    return getNumTaskTrackers( jobConf ) * jobConf.getInt( "mapred.tasktracker.reduce.tasks.maximum", 2 );
    }

  public static boolean hasNativeZlib()
    {
    return ZlibFactory.isNativeZlibLoaded( new JobConf() );
    }

  }
