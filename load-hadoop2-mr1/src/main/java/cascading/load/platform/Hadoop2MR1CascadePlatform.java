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
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProps;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.load.Options;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.collect.SpillableProps;
import cascading.tuple.collect.SpillableTupleList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Implementation of CascadeLoadPlatform for hadoop2-mr1.
 */
public class Hadoop2MR1CascadePlatform extends BaseHadoopCascadePlatform
  {

  private static final Logger LOG = Logger.getLogger( Hadoop2MR1CascadePlatform.class );

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

  }