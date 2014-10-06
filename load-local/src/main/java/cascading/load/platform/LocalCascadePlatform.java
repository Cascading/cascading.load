package cascading.load.platform;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.load.Options;
import cascading.load.util.Util;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class LocalCascadePlatform implements CascadingLoadPlatform
  {

  @Override
  public Tap newTap( Scheme scheme, String stringPath )
    {
    return new FileTap( scheme, stringPath );
    }

  @Override
  public Tap newTap( Scheme scheme, String stringPath, SinkMode sinkMode )
    {
    return new FileTap( scheme, stringPath, sinkMode );
    }

  @Override
  public TupleEntryCollector newTupleEntryCollector( Tap tap ) throws IOException
    {
    return tap.openForWrite( new LocalFlowProcess() );
    }

  @Override
  public Scheme newTextLine()
    {
    return new cascading.scheme.local.TextLine();
    }

  @Override
  public Scheme newTextLine( Fields sourceFields )
    {
    return new cascading.scheme.local.TextLine( sourceFields );
    }

  @Override
  public Scheme newTextLine( Fields sourceFields, Fields sinkFields )
    {
    return new cascading.scheme.local.TextLine( sourceFields, sinkFields );
    }

  @Override
  public FlowConnector newFlowConnector()
    {
    return new LocalFlowConnector();
    }

  @Override
  public FlowConnector newFlowConnector( Map<Object, Object> properties )
    {
    return new LocalFlowConnector( properties );
    }

  @Override
  public void writeDictionaryData( Tuple data, String path, int numberOfFiles ) throws IOException
    {
    Tap tap = newTap( newTextLine(), path );
    TupleEntryCollector writer = newTupleEntryCollector( tap );
    writer.add( data );
    writer.close();
    }

  @Override
  public Properties buildPlatformProperties( Options options )
    {
    return new Properties();
    }

  @Override
  public void cleanDirectories( String... paths ) throws IOException
    {
    for( String path : paths )
      Util.deleteRecursive( new File( path ) );
    }

  @Override
  public int getMaxConcurrentMappers()
    {
    return 1;
    }

  @Override
  public int getMaxConcurrentReducers()
    {
    return 1;
    }
  }
