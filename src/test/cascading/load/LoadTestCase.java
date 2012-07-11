/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load;

import java.util.Properties;

import cascading.PlatformTestCase;

/**
 *
 */
public class LoadTestCase extends PlatformTestCase
  {
  public LoadTestCase()
    {
    super( false );
    }

  public Properties getProperties()
    {
    Properties properties = new Properties();

    properties.putAll( super.getProperties() );

    return properties;
    }
  }
