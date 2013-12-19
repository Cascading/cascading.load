/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.load.common;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.load.Options;

/**
 *
 */
public abstract class Load
  {
  protected Options options;
  protected Properties properties;
  protected CascadeLoadPlatform platform;

  public Load( Options options, Properties properties )
    {
    this.options = options;
//    this.properties = new Properties( properties );
    this.properties = properties;
    this.platform = CascadeLoadPlatform.getPlatform( options );
    }

  public abstract Flow createFlow() throws Exception;

  public abstract String[] getInputPaths();

  public abstract String[] getOutputPaths();
  }
