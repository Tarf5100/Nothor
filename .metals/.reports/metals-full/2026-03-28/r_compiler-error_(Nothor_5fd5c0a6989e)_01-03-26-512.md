error id: 7B24F233620AC68017BC6B6C3469744D
file:///C:/MAMP/htdocs/Nothor/RDDOperations.scala
### dotty.tools.dotc.core.UnpicklingError: Could not read definition object Predef in <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar(scala/Predef.tasty). Caused by the following exception:
java.lang.AssertionError: assertion failed: `-Xread-docs` enabled, but no `docCtx` is set.

Run with -Ydebug-unpickling to see full stack trace.

occurred in the presentation compiler.



action parameters:
offset: 6673
uri: file:///C:/MAMP/htdocs/Nothor/RDDOperations.scala
text:
```scala
scala> :load RDDOperations.scala
Loading RDDOperations.scala...
import org.apache.spark.sql.{SparkSession, Row}
defined object RDDOperations

scala> RDDOperations.main(Array())
26/03/28 00:49:44 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
[Stage 1:>                                                       
                                                                 
26/03/28 00:49:51 ERROR Executor: Exception in task 1.0 in stage 2.0 (TID 6)
java.lang.IllegalArgumentException: Accident_Severity does not exist. Available: Accident_Index, 1st_Road_Class, 1st_Road_Number, 2nd_Road_Class, 2nd_Road_Number, Carriageway_Hazards, Did_Police_Officer_Attend_Scene_of_Accident, Junction_Control, Junction_Detail, Latitude, Longitude, Number_of_Casualties, Number_of_Vehicles, Pedestrian_Crossing-Human_Control, Pedestrian_Crossing-Physical_Facilities, Special_Conditions_at_Site, Speed_limit, Year, InScotland, Date_std, Time_std, Hour_of_Day, Road_Type_idx, Weather_Conditions_idx, Road_Surface_Conditions_idx, Light_Conditions_idx, Urban_or_Rural_Area_idx, Accident_Severity_idx, Day_of_Week_idx, High_Risk_Conditions, Time_Period_idx
        at org.apache.spark.sql.types.StructType.$anonfun$fieldIndex$1(StructType.scala:310)
        at scala.collection.immutable.HashMap$HashTrieMap.getOrElse0(HashMap.scala:596)
        at scala.collection.immutable.HashMap.getOrElse(HashMap.scala:73)
        at org.apache.spark.sql.types.StructType.fieldIndex(StructType.scala:309)
        at org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.fieldIndex(rows.scala:48)
        at org.apache.spark.sql.Row.getAs(Row.scala:372)
        at org.apache.spark.sql.Row.getAs$(Row.scala:372)        
        at org.apache.spark.sql.catalyst.expressions.GenericRow.getAs(rows.scala:27)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1(RDDOperations.scala:45)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1$adapted(RDDOperations.scala:44)
        at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:515)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
        at org.apache.spark.util.collection.ExternalSorter.insertAll(ExternalSorter.scala:197)
        at org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.scala:63)
        at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:104)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:54)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)   
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:621)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:624)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:834)       
26/03/28 00:49:51 ERROR Executor: Exception in task 3.0 in stage 2.0 (TID 8)
java.lang.IllegalArgumentException: Accident_Severity does not exist. Available: Accident_Index, 1st_Road_Class, 1st_Road_Number, 2nd_Road_Class, 2nd_Road_Number, Carriageway_Hazards, Did_Police_Officer_Attend_Scene_of_Accident, Junction_Control, Junction_Detail, Latitude, Longitude, Number_of_Casualties, Number_of_Vehicles, Pedestrian_Crossing-Human_Control, Pedestrian_Crossing-Physical_Facilities, Special_Conditions_at_Site, Speed_limit, Year, InScotland, Date_std, Time_std, Hour_of_Day, Road_Type_idx, Weather_Conditions_idx, Road_Surface_Conditions_idx, Light_Conditions_idx, Urban_or_Rural_Area_idx, Accident_Severity_idx, Day_of_Week_idx, High_Risk_Conditions, Time_Period_idx
        at org.apache.spark.sql.types.StructType.$anonfun$fieldIndex$1(StructType.scala:310)
        at scala.collection.immutable.HashMap$HashTrieMap.getOrElse0(HashMap.scala:596)
        at scala.collection.immutable.HashMap.getOrElse(HashMap.scala:73)
        at org.apache.spark.sql.types.StructType.fieldIndex(StructType.scala:309)
        at org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.fieldIndex(rows.scala:48)
        at org.apache.spark.sql.Row.getAs(Row.scala:372)
        at org.apache.spark.sql.Row.getAs$(Row.scala:372)        
        at org.apache.spark.sql.catalyst.expressions.GenericRow.getAs(rows.scala:27)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1(RDDOperations.scala:45)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1$adapted(RDDOperations.scala:44)
        at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:515)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
        at org.apache.spark.util.collection.ExternalSorter.insertAll(ExternalSorter.scala:197)
        at org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.scala:63)
        at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:104)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:54)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)   
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:621)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:624)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.ru@@n(Thread.java:834)       
26/03/28 00:49:51 ERROR Executor: Exception in task 2.0 in stage 2.0 (TID 7)
java.lang.IllegalArgumentException: Accident_Severity does not exist. Available: Accident_Index, 1st_Road_Class, 1st_Road_Number, 2nd_Road_Class, 2nd_Road_Number, Carriageway_Hazards, Did_Police_Officer_Attend_Scene_of_Accident, Junction_Control, Junction_Detail, Latitude, Longitude, Number_of_Casualties, Number_of_Vehicles, Pedestrian_Crossing-Human_Control, Pedestrian_Crossing-Physical_Facilities, Special_Conditions_at_Site, Speed_limit, Year, InScotland, Date_std, Time_std, Hour_of_Day, Road_Type_idx, Weather_Conditions_idx, Road_Surface_Conditions_idx, Light_Conditions_idx, Urban_or_Rural_Area_idx, Accident_Severity_idx, Day_of_Week_idx, High_Risk_Conditions, Time_Period_idx
        at org.apache.spark.sql.types.StructType.$anonfun$fieldIndex$1(StructType.scala:310)
        at scala.collection.immutable.HashMap$HashTrieMap.getOrElse0(HashMap.scala:596)
        at scala.collection.immutable.HashMap.getOrElse(HashMap.scala:73)
        at org.apache.spark.sql.types.StructType.fieldIndex(StructType.scala:309)
        at org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.fieldIndex(rows.scala:48)
        at org.apache.spark.sql.Row.getAs(Row.scala:372)
        at org.apache.spark.sql.Row.getAs$(Row.scala:372)        
        at org.apache.spark.sql.catalyst.expressions.GenericRow.getAs(rows.scala:27)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1(RDDOperations.scala:45)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1$adapted(RDDOperations.scala:44)
        at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:515)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
        at org.apache.spark.util.collection.ExternalSorter.insertAll(ExternalSorter.scala:197)
        at org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.scala:63)
        at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:104)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:54)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)   
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:621)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:624)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:834)       
26/03/28 00:49:51 ERROR Executor: Exception in task 0.0 in stage 2.0 (TID 5)
java.lang.IllegalArgumentException: Accident_Severity does not exist. Available: Accident_Index, 1st_Road_Class, 1st_Road_Number, 2nd_Road_Class, 2nd_Road_Number, Carriageway_Hazards, Did_Police_Officer_Attend_Scene_of_Accident, Junction_Control, Junction_Detail, Latitude, Longitude, Number_of_Casualties, Number_of_Vehicles, Pedestrian_Crossing-Human_Control, Pedestrian_Crossing-Physical_Facilities, Special_Conditions_at_Site, Speed_limit, Year, InScotland, Date_std, Time_std, Hour_of_Day, Road_Type_idx, Weather_Conditions_idx, Road_Surface_Conditions_idx, Light_Conditions_idx, Urban_or_Rural_Area_idx, Accident_Severity_idx, Day_of_Week_idx, High_Risk_Conditions, Time_Period_idx
        at org.apache.spark.sql.types.StructType.$anonfun$fieldIndex$1(StructType.scala:310)
        at scala.collection.immutable.HashMap$HashTrieMap.getOrElse0(HashMap.scala:596)
        at scala.collection.immutable.HashMap.getOrElse(HashMap.scala:73)
        at org.apache.spark.sql.types.StructType.fieldIndex(StructType.scala:309)
        at org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.fieldIndex(rows.scala:48)
        at org.apache.spark.sql.Row.getAs(Row.scala:372)
        at org.apache.spark.sql.Row.getAs$(Row.scala:372)        
        at org.apache.spark.sql.catalyst.expressions.GenericRow.getAs(rows.scala:27)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1(RDDOperations.scala:45)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1$adapted(RDDOperations.scala:44)
        at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:515)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
        at org.apache.spark.util.collection.ExternalSorter.insertAll(ExternalSorter.scala:197)
        at org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.scala:63)
        at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:104)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:54)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)   
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:621)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:624)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:834)       
[Stage 2:>                                                       
26/03/28 00:49:51 WARN TaskSetManager: Lost task 1.0 in stage 2.0 (TID 6) (Maha executor driver): java.lang.IllegalArgumentException: Accident_Severity does not exist. Available: Accident_Index, 1st_Road_Class, 1st_Road_Number, 2nd_Road_Class, 2nd_Road_Number, Carriageway_Hazards, Did_Police_Officer_Attend_Scene_of_Accident, Junction_Control, Junction_Detail, Latitude, Longitude, Number_of_Casualties, Number_of_Vehicles, Pedestrian_Crossing-Human_Control, Pedestrian_Crossing-Physical_Facilities, Special_Conditions_at_Site, Speed_limit, Year, InScotland, Date_std, Time_std, Hour_of_Day, Road_Type_idx, Weather_Conditions_idx, Road_Surface_Conditions_idx, Light_Conditions_idx, Urban_or_Rural_Area_idx, Accident_Severity_idx, Day_of_Week_idx, High_Risk_Conditions, Time_Period_idx
        at org.apache.spark.sql.types.StructType.$anonfun$fieldIndex$1(StructType.scala:310)
        at scala.collection.immutable.HashMap$HashTrieMap.getOrElse0(HashMap.scala:596)
        at scala.collection.immutable.HashMap.getOrElse(HashMap.scala:73)
        at org.apache.spark.sql.types.StructType.fieldIndex(StructType.scala:309)
        at org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.fieldIndex(rows.scala:48)
        at org.apache.spark.sql.Row.getAs(Row.scala:372)
        at org.apache.spark.sql.Row.getAs$(Row.scala:372)        
        at org.apache.spark.sql.catalyst.expressions.GenericRow.getAs(rows.scala:27)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1(RDDOperations.scala:45)
        at $line15.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$RDDOperations$.$anonfun$main$1$adapted(RDDOperations.scala:44)
        at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:515)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
        at org.apache.spark.util.collection.ExternalSorter.insertAll(ExternalSorter.scala:197)
        at org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.scala:63)
        at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:104)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:54)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)   
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:621)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:624)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:834)       

26/03/28 00:49:51 ERROR TaskSetManager: Task 1 in stage 2.0 failed 1 times; aborting job
org.apache.spark.SparkException: Job aborted due to stage failure: Task 1 in stage 2.0 failed 1 times, most recent failure: Lost task 1.0 in stage 2.0 (TID 6) (Maha executor driver): java.lang.IllegalArgumentException: Accident_Severity does not exist. Available: Accident_Index, 1st_Road_Class, 1st_Road_Number, 2nd_Road_Class, 2nd_Road_Number, Carriageway_Hazards, Did_Police_Officer_Attend_Scene_of_Accident, Junction_Control, Junction_Detail, Latitude, Longitude, Number_of_Casualties, Number_of_Vehicles, Pedestrian_Crossing-Human_Control, Pedestrian_Crossing-Physical_Facilities, Special_Conditions_at_Site, Speed_limit, Year, InScotland, Date_std, Time_std, Hour_of_Day, Road_Type_idx, Weather_Conditions_idx, Road_Surface_Conditions_idx, Light_Conditions_idx, Urban_or_Rural_Area_idx, Accident_Severity_idx, Day_of_Week_idx, High_Risk_Conditions, Time_Period_idx
        at org.apache.spark.sql.types.StructType.$anonfun$fieldIndex$1(StructType.scala:310)
        at scala.collection.immutable.HashMap$HashTrieMap.getOrElse0(HashMap.scala:596)
        at scala.collection.immutable.HashMap.getOrElse(HashMap.scala:73)
        at org.apache.spark.sql.types.StructType.fieldIndex(StructType.scala:309)
        at org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.fieldIndex(rows.scala:48)
        at org.apache.spark.sql.Row.getAs(Row.scala:372)
        at org.apache.spark.sql.Row.getAs$(Row.scala:372)        
        at org.apache.spark.sql.catalyst.expressions.GenericRow.getAs(rows.scala:27)
        at RDDOperations$.$anonfun$main$1(RDDOperations.scala:45)
        at RDDOperations$.$anonfun$main$1$adapted(RDDOperations.scala:44)
        at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:515)
        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
        at org.apache.spark.util.collection.ExternalSorter.insertAll(ExternalSorter.scala:197)
        at org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.scala:63)
        at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:104)
        at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:54)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)   
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:621)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:624)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:834)       

Driver stacktrace:
  at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2898)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2834)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2833)
  at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
  at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
  at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
  at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2833)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1253)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1253)
  at scala.Option.foreach(Option.scala:407)
  at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1253)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3102)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:3036)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:3025)
  at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
  at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:995)
  at org.apache.spark.SparkContext.runJob(SparkContext.scala:2393)
  at org.apache.spark.SparkContext.runJob(SparkContext.scala:2414)
  at org.apache.spark.SparkContext.runJob(SparkContext.scala:2433)
  at org.apache.spark.SparkContext.runJob(SparkContext.scala:2458)
  at org.apache.spark.rdd.RDD.$anonfun$collect$1(RDD.scala:1049) 
  at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
  at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
  at org.apache.spark.rdd.RDD.withScope(RDD.scala:410)
  at org.apache.spark.rdd.RDD.collect(RDD.scala:1048)
  at org.apache.spark.RangePartitioner$.sketch(Partitioner.scala:320)
  at org.apache.spark.RangePartitioner.<init>(Partitioner.scala:187)
  at org.apache.spark.RangePartitioner.<init>(Partitioner.scala:167)
  at org.apache.spark.rdd.OrderedRDDFunctions.$anonfun$sortByKey$1(OrderedRDDFunctions.scala:64)
  at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
  at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
  at org.apache.spark.rdd.RDD.withScope(RDD.scala:410)
  at org.apache.spark.rdd.OrderedRDDFunctions.sortByKey(OrderedRDDFunctions.scala:63)
  at org.apache.spark.rdd.RDD.$anonfun$sortBy$1(RDD.scala:680)   
  at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
  at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
  at org.apache.spark.rdd.RDD.withScope(RDD.scala:410)
  at org.apache.spark.rdd.RDD.sortBy(RDD.scala:681)
  at RDDOperations$.main(RDDOperations.scala:50)
  ... 47 elided
Caused by: java.lang.IllegalArgumentException: Accident_Severity does not exist. Available: Accident_Index, 1st_Road_Class, 1st_Road_Number, 2nd_Road_Class, 2nd_Road_Number, Carriageway_Hazards, Did_Police_Officer_Attend_Scene_of_Accident, Junction_Control, Junction_Detail, Latitude, Longitude, Number_of_Casualties, Number_of_Vehicles, Pedestrian_Crossing-Human_Control, Pedestrian_Crossing-Physical_Facilities, Special_Conditions_at_Site, Speed_limit, Year, InScotland, Date_std, Time_std, Hour_of_Day, Road_Type_idx, Weather_Conditions_idx, Road_Surface_Conditions_idx, Light_Conditions_idx, Urban_or_Rural_Area_idx, Accident_Severity_idx, Day_of_Week_idx, High_Risk_Conditions, Time_Period_idx
  at org.apache.spark.sql.types.StructType.$anonfun$fieldIndex$1(StructType.scala:310)
  at scala.collection.immutable.HashMap$HashTrieMap.getOrElse0(HashMap.scala:596)
  at scala.collection.immutable.HashMap.getOrElse(HashMap.scala:73)
  at org.apache.spark.sql.types.StructType.fieldIndex(StructType.scala:309)
  at org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.fieldIndex(rows.scala:48)
  at org.apache.spark.sql.Row.getAs(Row.scala:372)
  at org.apache.spark.sql.Row.getAs$(Row.scala:372)
  at org.apache.spark.sql.catalyst.expressions.GenericRow.getAs(rows.scala:27)
  at RDDOperations$.$anonfun$main$1(RDDOperations.scala:45)      
  at RDDOperations$.$anonfun$main$1$adapted(RDDOperations.scala:44)
  at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:515)
  at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
  at org.apache.spark.util.collection.ExternalSorter.insertAll(ExternalSorter.scala:197)
  at org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.scala:63)
  at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
  at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:104)
  at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:54)
  at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
  at org.apache.spark.scheduler.Task.run(Task.scala:141)
  at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:621)
  at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
  at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
  at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
  at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:624)
  at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
  at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
  at java.base/java.lang.Thread.run(Thread.java:834)

scala>
```


presentation compiler configuration:
Scala version: 3.8.2-bin-nonbootstrapped
Classpath:
<WORKSPACE>\.scala-build\Nothor_d5c0a6989e\classes\main [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala3-library_3\3.8.2\scala3-library_3-3.8.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sourcegraph\semanticdb-javac\0.10.0\semanticdb-javac-0.10.0.jar [exists ], <WORKSPACE>\.scala-build\Nothor_d5c0a6989e\classes\main\META-INF\best-effort [missing ]
Options:
-Xsemanticdb -sourceroot <WORKSPACE> -release 11 -Ywith-best-effort-tasty




#### Error stacktrace:

```

```
#### Short summary: 

dotty.tools.dotc.core.UnpicklingError: Could not read definition object Predef in <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\3.8.2\scala-library-3.8.2.jar(scala/Predef.tasty). Caused by the following exception:
java.lang.AssertionError: assertion failed: `-Xread-docs` enabled, but no `docCtx` is set.

Run with -Ydebug-unpickling to see full stack trace.