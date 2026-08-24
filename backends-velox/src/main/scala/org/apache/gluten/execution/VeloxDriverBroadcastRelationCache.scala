/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.velox.VeloxBackendSettings
import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.expression.ConverterUtils

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.types.{StructField, StructType}

import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause, RemovalListener, Weigher}
import com.github.benmanes.caffeine.cache.stats.CacheStats

import java.util.IdentityHashMap
import java.util.concurrent.TimeUnit

/**
 * Identifies a broadcast hash table that was built on the driver.
 *
 * Every field is independent of the query instance the build side came from: the plan is
 * canonicalized and the join keys are normalized against the build side output, so two structurally
 * identical broadcast build sides taken from two different queries map to the same key. Fields that
 * are not part of this key must not influence the content of the built hash table.
 */
case class DriverBroadcastRelationKey(
    canonicalizedBuildPlan: SparkPlan,
    buildSchema: StructType,
    normalizedJoinKeys: Seq[Expression],
    normalizedRelationKeys: Seq[Expression],
    substraitJoinType: Int,
    buildRight: Boolean,
    hasMixedFiltCondition: Boolean,
    isExistenceJoin: Boolean,
    filterBuildColumnOrdinals: Seq[Int],
    filterPropagatesNulls: Boolean,
    isNullAwareAntiJoin: Boolean,
    bloomFilterPushdownSize: Long,
    droppedDuplicates: Boolean,
    useOffheapRelation: Boolean)

/**
 * A broadcast relation that is kept alive on the driver so that following queries can reuse it,
 * together with the statistics of the job that produced it. The statistics are needed because a
 * query that hits the cache does not run the build side job and still has to report the same
 * `numOutputRows` / `dataSize` metrics as a query that builds the relation.
 */
class CachedBroadcastRelation(
    val relation: BuildSideRelation,
    val numRows: Long,
    val rawSize: Long,
    val buildThreads: Int,
    val serializedSize: Long) {

  @volatile private var broadcasted: Broadcast[Any] = _

  /** The broadcast that was created for [[relation]], if it has already been broadcast once. */
  def broadcast: Option[Broadcast[Any]] = Option(broadcasted)

  def setBroadcast(value: Broadcast[Any]): Unit = broadcasted = value
}

/**
 * Driver-side cache of the broadcast relations produced by driver-side broadcast hash table build.
 *
 * With `spark.gluten.sql.columnar.backend.velox.driverSideBroadcastHashTableBuild` enabled the hash
 * table is built and serialized once per `BroadcastExchangeExec` instance, which means every query
 * rebuilds the hash tables of its build sides even when a previous query has just built the very
 * same one. That is the common case for workloads that run the same queries over and over, e.g. the
 * concurrent streams of a TPC-DS throughput run.
 *
 * This cache keys the built relation by the canonicalized build side plan (see
 * [[DriverBroadcastRelationKey]]) instead of by the exchange instance, so a later query can skip:
 *   - collecting the build side to the driver,
 *   - building and serializing the hash table,
 *   - broadcasting the serialized hash table, when the very same relation object is broadcast again
 *     the broadcast created the first time is reused as well.
 *
 * The off-heap memory of a cached serialized hash table is released by `UnsafeByteArray#finalize`
 * once neither this cache nor any broadcast refers to it, so eviction only drops the reference and
 * never frees memory that a running query may still be using.
 */
object VeloxDriverBroadcastRelationCache extends Logging {

  // Caffeine weights are ints, so the cache is bounded in KiB rather than in bytes.
  private val WEIGHT_UNIT = 1024L

  private lazy val cache: Cache[DriverBroadcastRelationKey, CachedBroadcastRelation] = {
    val maxSize = VeloxConfig.get.driverSideBroadcastHashTableCacheMaxSize
    val expiredTime = SparkEnv.get.conf.getLong(
      VeloxBackendSettings.GLUTEN_VELOX_BROADCAST_CACHE_EXPIRED_TIME,
      VeloxBackendSettings.GLUTEN_VELOX_BROADCAST_CACHE_EXPIRED_TIME_DEFAULT
    )
    logInfo(
      s"Creating driver-side broadcast relation cache, maxSize=$maxSize bytes, " +
        s"expiredTime=$expiredTime seconds")
    Caffeine.newBuilder
      .expireAfterAccess(expiredTime, TimeUnit.SECONDS)
      .maximumWeight(math.max(1L, maxSize / WEIGHT_UNIT))
      .weigher(new Weigher[DriverBroadcastRelationKey, CachedBroadcastRelation] {
        override def weigh(
            key: DriverBroadcastRelationKey,
            value: CachedBroadcastRelation): Int = {
          // A cached relation keeps both the serialized hash table and the collected build side
          // batches it was built from alive, the latter is still needed by DPP.
          val bytes = value.serializedSize + value.rawSize
          math.max(1L, bytes / WEIGHT_UNIT).min(Int.MaxValue.toLong).toInt
        }
      })
      .removalListener(new RemovalListener[DriverBroadcastRelationKey, CachedBroadcastRelation] {
        override def onRemoval(
            key: DriverBroadcastRelationKey,
            value: CachedBroadcastRelation,
            cause: RemovalCause): Unit = {
          if (value != null) {
            broadcastIndex.synchronized(broadcastIndex.remove(value.relation))
            logInfo(
              s"Evicted a cached broadcast relation of ${value.serializedSize} bytes, " +
                s"cause: $cause")
          }
        }
      })
      .recordStats()
      .build[DriverBroadcastRelationKey, CachedBroadcastRelation]()
  }

  // Maps a cached relation to its entry by identity, used to reuse the broadcast that was created
  // for the relation. Kept in sync with `cache` by the removal listener above.
  private val broadcastIndex =
    new IdentityHashMap[BuildSideRelation, CachedBroadcastRelation]()

  def enabled: Boolean = {
    val conf = VeloxConfig.get
    conf.enableDriverSideBroadcastHashTableBuild && conf.enableDriverSideBroadcastHashTableCache
  }

  /**
   * Build the cache key of a broadcast build side, or return None if the build side must not be
   * shared between queries.
   *
   * @param buildPlan
   *   the plan whose output is broadcast, after the pre-projection rewriting
   * @param buildOutput
   *   the output of `buildPlan`
   * @param relationKeys
   *   the build keys carried by the build side relation, empty if the relation takes its keys from
   *   the join context
   * @param context
   *   the join context the hash table is built with
   * @param useOffheapRelation
   *   whether the build side batches are kept off-heap
   */
  def keyOf(
      buildPlan: SparkPlan,
      buildOutput: Seq[Attribute],
      relationKeys: Seq[Expression],
      context: BroadcastHashJoinContext,
      useOffheapRelation: Boolean): Option[DriverBroadcastRelationKey] = {
    if (!isCacheable(buildPlan)) {
      logInfo(
        "The broadcast build side is not shareable between queries, " +
          "skipping the driver-side broadcast relation cache")
      return None
    }

    // `filterBuildColumns` carries expression ids, which differ between two instances of the same
    // query. Turn the names back into positions in the build side output, and give up caching if
    // any of them cannot be resolved.
    val ordinalByName = buildOutput.zipWithIndex.map {
      case (attr, ordinal) => ConverterUtils.genColumnNameWithExprId(attr) -> ordinal
    }.toMap
    val filterOrdinals = context.filterBuildColumns.map(ordinalByName.get)
    if (filterOrdinals.exists(_.isEmpty)) {
      logInfo(
        "Failed to resolve the filter build columns of the broadcast build side, " +
          "skipping the driver-side broadcast relation cache")
      return None
    }

    Some(
      DriverBroadcastRelationKey(
        canonicalizedBuildPlan = buildPlan.canonicalized,
        buildSchema = StructType(
          buildOutput.map(attr => StructField(attr.name, attr.dataType, attr.nullable))),
        normalizedJoinKeys =
          context.buildSideJoinKeys.map(QueryPlan.normalizeExpressions(_, buildOutput)),
        normalizedRelationKeys = relationKeys.map(QueryPlan.normalizeExpressions(_, buildOutput)),
        substraitJoinType = context.substraitJoinType.ordinal(),
        buildRight = context.buildRight,
        hasMixedFiltCondition = context.hasMixedFiltCondition,
        isExistenceJoin = context.isExistenceJoin,
        filterBuildColumnOrdinals = filterOrdinals.flatten.toSeq.sorted,
        filterPropagatesNulls = context.filterPropagatesNulls,
        isNullAwareAntiJoin = context.isNullAwareAntiJoin,
        bloomFilterPushdownSize = context.bloomFilterPushdownSize,
        droppedDuplicates = context.droppedDuplicates,
        useOffheapRelation = useOffheapRelation
      ))
  }

  def getIfPresent(key: DriverBroadcastRelationKey): Option[CachedBroadcastRelation] = {
    if (!enabled) {
      None
    } else {
      Option(cache.getIfPresent(key))
    }
  }

  def put(
      key: DriverBroadcastRelationKey,
      relation: BuildSideRelation,
      numRows: Long,
      rawSize: Long,
      buildThreads: Int,
      serializedSize: Long): Unit = {
    val cached =
      new CachedBroadcastRelation(relation, numRows, rawSize, buildThreads, serializedSize)
    // Insert into `cache` first so that any removal listener triggered by a replaced entry fires
    // before the new entry is visible in `broadcastIndex`. Reversing the order would leave a
    // window where the removal listener could remove the new entry from `broadcastIndex`.
    cache.put(key, cached)
    broadcastIndex.synchronized(broadcastIndex.put(relation, cached))
  }

  /** The broadcast that was created for a cached relation, if any. */
  def cachedBroadcast(relation: BuildSideRelation): Option[Broadcast[Any]] =
    entryOf(relation).flatMap(_.broadcast)

  /** Remember the broadcast created for a relation, so that a later query can reuse it. */
  def registerBroadcast(relation: BuildSideRelation, broadcasted: Broadcast[Any]): Unit =
    entryOf(relation).foreach(_.setBroadcast(broadcasted))

  private def entryOf(relation: BuildSideRelation): Option[CachedBroadcastRelation] = {
    if (relation == null) {
      None
    } else {
      broadcastIndex.synchronized(Option(broadcastIndex.get(relation)))
    }
  }

  /**
   * A build side can only be shared between queries if it produces the same data every time it is
   * evaluated. Non-deterministic expressions and subqueries, including the runtime filters of
   * dynamic partition pruning, rule the build side out.
   */
  private def isCacheable(plan: SparkPlan): Boolean = {
    plan
      .find {
        p =>
          p.expressions.exists {
            e => !e.deterministic || e.find(_.isInstanceOf[PlanExpression[_]]).isDefined
          }
      }
      .isEmpty
  }

  def size(): Long = {
    // Run the pending maintenance work first, so that the returned size is exact.
    cache.cleanUp()
    cache.estimatedSize()
  }

  def stats(): CacheStats = cache.stats()

  def cleanAll(): Unit = {
    // Clear `broadcastIndex` first so that the removal listener fired by `cleanUp` does not
    // observe stale entries after the index has been cleared.
    broadcastIndex.synchronized(broadcastIndex.clear())
    cache.invalidateAll()
    cache.cleanUp()
  }
}
