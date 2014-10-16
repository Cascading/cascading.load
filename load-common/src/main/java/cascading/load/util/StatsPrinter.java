/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.util;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.flow.SliceCounters;
import cascading.load.platform.CascadingLoadPlatform;
import cascading.stats.CascadeStats;
import cascading.stats.CascadingStats;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;

/**
 *
 */
public class StatsPrinter
  {
  CascadingLoadPlatform platform;

  public StatsPrinter( CascadingLoadPlatform platform )
    {
    this.platform = platform;
    }

  public void printStats( PrintWriter writer, CascadeStats cascadeStats, boolean singlelineStats )
    {
    if( singlelineStats )
      {
      writer.printf( "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%n",
        "platform",
        "type",
        "name",
        "status",
        "start-time",
        "start-time-long",
        "finished-time",
        "finished-time-long",
        "duration",
        "duration-long",
        "process-duration",
        "process-duration-long",
        "read-duration",
        "read-duration-long",
        "write-duration",
        "write-duration-long",
        "user-duration",
        "user-duration-long",
        "cpu-duration",
        "cpu-duration-long",
        "children"
      );
      }

    printCascadeStats( writer, cascadeStats, singlelineStats );
    }

  public void printCascadeStats( PrintWriter writer, CascadeStats cascadeStats, boolean singlelineStats )
    {
    printSummaryFor( writer, "Cascade", cascadeStats, null, singlelineStats );

    Collection<FlowStats> flowStats = cascadeStats.getChildren();

    for( FlowStats flowStat : flowStats )
      {
      if( !singlelineStats )
        writer.println();

      printFlowStats( writer, flowStat, singlelineStats );
      }
    }

  public void printFlowStats( PrintWriter writer, FlowStats flowStat, boolean singlelineStats )
    {
    printSummaryFor( writer, "Flow", flowStat, null, singlelineStats );

    Collection<FlowStepStats> stepStats = flowStat.getChildren();

    for( FlowStepStats stepStat : stepStats )
      {
      if( !singlelineStats )
        writer.println();

      printStepStats( writer, stepStat, flowStat, singlelineStats );
      }
    }

  public void printStepStats( PrintWriter writer, FlowStepStats stepStat, FlowStats flowStat, boolean singlelineStats )
    {
    String overrideName = flowStat == null ? null : getStepStatsName( stepStat, flowStat.getName(), singlelineStats );

    printSummaryFor( writer, "Step", stepStat, overrideName, singlelineStats );

    Collection<FlowNodeStats> nodeStats = stepStat.getChildren();

    for( FlowNodeStats nodeStat : nodeStats )
      {
      if( !singlelineStats )
        writer.println();

      printNodeStats( writer, nodeStat, stepStat, singlelineStats );
      }
    }

  private void printNodeStats( PrintWriter writer, FlowNodeStats nodeStat, FlowStepStats stepStat, boolean singlelineStats )
    {
    String overrideName = nodeStat == null ? null : getStepStatsName( nodeStat, stepStat.getName(), singlelineStats );

    printSummaryFor( writer, "Node", nodeStat, overrideName, singlelineStats );
    }

  private void printSummaryFor( PrintWriter writer, String type, CascadingStats cascadingStats, String overrideName, boolean singlelineStats )
    {
    if( singlelineStats )
      printRecordSummaryFor( writer, type, cascadingStats, overrideName );
    else
      printDisplaySummaryFor( writer, type, cascadingStats, overrideName );
    }

  private void printRecordSummaryFor( PrintWriter writer, String type, CascadingStats cascadingStats, String overrideName )
    {
    int childCount = 0;

    if( !cascadingStats.getChildren().isEmpty() )
      childCount = cascadingStats.getChildren().size();

    long duration = cascadingStats.getDuration() / 1000;
    long processDuration = cascadingStats.getCounterValue( SliceCounters.Process_Duration ) / 1000;
    long readDuration = cascadingStats.getCounterValue( SliceCounters.Read_Duration ) / 1000;
    long writeDuration = cascadingStats.getCounterValue( SliceCounters.Write_Duration ) / 1000;
    long userDuration = processDuration - readDuration - writeDuration;
    long cpuDuration = platform.getCPUMillis( cascadingStats ) / 1000;

    writer.printf( "%s\t%s\t%s\t%s\t%tT\t%d\t%tT\t%d\t%s\t%d\t%s\t%d\t%s\t%d\t%s\t%d\t%s\t%d\t%s\t%d\t%d%n",
      platform.getName(),
      type,
      overrideName == null ? cascadingStats.getName() : overrideName,
      cascadingStats.getStatus(),
      cascadingStats.getStartTime(),
      cascadingStats.getStartTime(),
      cascadingStats.getFinishedTime(),
      cascadingStats.getFinishedTime(),
      String.format( "%d:%02d:%02d", duration / 3600, duration % 3600 / 60, duration % 60 ),
      duration,
      String.format( "%d:%02d:%02d", processDuration / 3600, processDuration % 3600 / 60, processDuration % 60 ),
      processDuration,
      String.format( "%d:%02d:%02d", readDuration / 3600, readDuration % 3600 / 60, readDuration % 60 ),
      readDuration,
      String.format( "%d:%02d:%02d", writeDuration / 3600, writeDuration % 3600 / 60, writeDuration % 60 ),
      writeDuration,
      String.format( "%d:%02d:%02d", userDuration / 3600, userDuration % 3600 / 60, userDuration % 60 ),
      userDuration,
      String.format( "%d:%02d:%02d", cpuDuration / 3600, cpuDuration % 3600 / 60, cpuDuration % 60 ),
      cpuDuration,
      childCount
    );

    writer.flush();
    }

  private void printDisplaySummaryFor( PrintWriter writer, String type, CascadingStats cascadingStats, String overrideName )
    {
    writer.printf( "%s: %s%n", type, overrideName == null ? cascadingStats.getName() : overrideName );

    writer.printf( "  finish status: %s%n", cascadingStats.getStatus() );
    writer.printf( "  start:    %tT%n", cascadingStats.getStartTime() );
    writer.printf( "  finished: %tT%n", cascadingStats.getFinishedTime() );

    long duration = cascadingStats.getDuration() / 1000;

    writer.printf( "  duration: %d:%02d:%02d%n", duration / 3600, duration % 3600 / 60, duration % 60 );

    long processDuration = cascadingStats.getCounterValue( SliceCounters.Process_Duration ) / 1000;

    writer.printf( "  process duration: %d:%02d:%02d%n", processDuration / 3600, processDuration % 3600 / 60, processDuration % 60 );

    long readDuration = cascadingStats.getCounterValue( SliceCounters.Read_Duration ) / 1000;

    writer.printf( "  read duration: %d:%02d:%02d%n", readDuration / 3600, readDuration % 3600 / 60, readDuration % 60 );

    long writeDuration = cascadingStats.getCounterValue( SliceCounters.Write_Duration ) / 1000;

    writer.printf( "  write duration: %d:%02d:%02d%n", writeDuration / 3600, writeDuration % 3600 / 60, writeDuration % 60 );

    long userDuration = processDuration - readDuration - writeDuration;

    writer.printf( "  user duration: %d:%02d:%02d%n", userDuration / 3600, userDuration % 3600 / 60, userDuration % 60 );

    if( !cascadingStats.getChildren().isEmpty() )
      writer.printf( "  num children: %d%n", cascadingStats.getChildren().size() );

    writer.flush();
    }

  private static final Pattern stepStatSeqNumPatt = Pattern.compile( "^(\\(\\d+/\\d+\\)) " );

  private static String getStepStatsName( CascadingStats stepStat, String parent, boolean uniqueName )
    {
    String name = stepStat.getName();

    if( !uniqueName )
      return name;

    Matcher matchSeqNum = stepStatSeqNumPatt.matcher( name );

    if( matchSeqNum.find() )
      return parent + " " + matchSeqNum.group();

    return parent + name;
    }
  }
