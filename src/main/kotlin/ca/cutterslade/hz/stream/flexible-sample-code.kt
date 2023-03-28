package ca.cutterslade.hz.stream

import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.map.IMap
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import mu.KotlinLogging
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

private val logger = KotlinLogging.logger {}

private enum class TestCase(
  val writeKeyCount: Int,
  val generatorDelay: Duration,
  val readKeyCount: Int,
  val readersPerKey: Int,
  val testDuration: Duration,
  val filterStrategy: FilterStrategy,
) {
  /**
   * This test adds a single entry to a map, then updates that entry every
   * second. These updates are observed by single jet job.
   *
   * This case completes as expected.
   */
  OneEntryOneReaderJournalPredicate(1, 1.seconds, 1, 1, 1.minutes, FilterStrategy.JournalPredicate),

  /**
   * This test adds a single entry to a map, then updates that entry every
   * second. These updates are observed by ten concurrent jet jobs.
   *
   * This case completes as expected.
   */
  OneEntryOneByTenReadersJournalPredicate(1, 1.seconds, 1, 10, 1.minutes, FilterStrategy.JournalPredicate),

  /**
   * This test adds ten entries to a map, then updates each one every second.
   * Updates to one of the keys are observed by ten concurrent jet jobs.
   *
   * This case completes as expected.
   */
  TenEntriesOneByTenReadersJournalPredicate(10, 1.seconds, 1, 10, 1.minutes, FilterStrategy.JournalPredicate),

  /**
   * This test adds ten entries to a map, then updates each one every second.
   * Updates to two of the keys are observed by one jet jobs per key.
   *
   * This case typically sees a very limited number of entries.
   */
  TwoEntriesTwoReadersJournalPredicate(2, 1.seconds, 2, 1, 1.minutes, FilterStrategy.JournalPredicate),

  /**
   * This test replicates the previous case, but moves the event predicate
   * from the journal source into the Jet stream.
   *
   * This case completes as expected.
   */
  TwoEntriesTwoReadersJetStream(2, 1.seconds, 2, 1, 1.minutes, FilterStrategy.JetStream),

  /**
   * This test increases the throughput of the previous test by using 10 map
   * entries, reading all 10 entries in ten concurrent jet jobs each, for 100
   * total jet jobs.
   *
   * This case completes as expected.
   */
  TenEntriesTenByTenReadersJetStream(10, 1.seconds, 10, 10, 1.minutes, FilterStrategy.JetStream),

  /**
   * This test increases the throughput of the previous test by using 10 map
   * entries, reading all 10 entries in ten concurrent jet jobs each, for 100
   * total jet jobs.
   *
   * This case completes as expected.
   */
  TenEntriesTenByTenReadersFastJetStream(10, 100.milliseconds, 10, 10, 1.minutes, FilterStrategy.JetStream),

  TwoEntriesTwoReadersJournalProjection(2, 1.seconds, 2, 1, 1.minutes, FilterStrategy.JournalProjection),
  TenEntriesTenByTenReadersJournalProjection(10, 1.seconds, 10, 10, 1.minutes, FilterStrategy.JournalProjection),
  TenEntriesTenByTenReadersFastJournalProjection(
    10,
    100.milliseconds,
    10,
    10,
    1.minutes,
    FilterStrategy.JournalProjection
  ),

  ;

  init {
    check(writeKeyCount > 0) { "writeKeyCount ($writeKeyCount) must be greater than zero" }
    check(generatorDelay.isPositive()) { "generatorDelay ($generatorDelay) greater than zero" }
    check(readKeyCount > 0) { "readKeyCount ($readKeyCount) must be greater than zero" }
    check(writeKeyCount >= readKeyCount) { "readKeyCount ($readKeyCount) must be less than or equal to writeKeyCount ($writeKeyCount)" }
    check(readersPerKey > 0) { "readersPerKey ($readersPerKey) must be greater than zero" }
    check(testDuration.isPositive()) { "testDuration ($testDuration) must be greater than zero" }
  }
}

private val activeTestCases = listOf(
  TestCase.TwoEntriesTwoReadersJournalPredicate
)

suspend fun main() {
  val hz = Hazelcast.newHazelcastInstance(hazelcastConfig())
  try {
    activeTestCases.forEachIndexed { index, inputs ->
      logger.info { "EXECUTING TEST CASE $index: $inputs" }
      supervisorScope {
        launch { testHazelcastStreaming(hz, "map-for-test-$index", inputs) }
      }
    }
  } finally {
    hz.shutdown()
  }
}

private suspend fun testHazelcastStreaming(
  hazelcast: HazelcastInstance,
  mapName: String,
  testCase: TestCase,
) {
  val map = hazelcast.getMap<String, String>(mapName)
  try {
    coroutineScope {
      val keys = launchGenerators(map, testCase).subList(0, testCase.readKeyCount)
      launchCollectors(hazelcast, mapName, keys, testCase)
      delay(testCase.testDuration)
      cancel()
    }
  } finally {
    map.clear()
  }
}

private fun CoroutineScope.launchGenerators(map: IMap<String, String>, testCase: TestCase): List<String> {
  val keys = (1..testCase.writeKeyCount).map { "key-$it" }
  keys.forEach { launch { generateEntries(map, it, testCase.generatorDelay) } }
  return keys
}

private suspend fun generateEntries(map: IMap<String, String>, key: String, delay: Duration) {
  var value = 0L
  while (currentCoroutineContext().isActive) {
    delay(delay)
    val v = "value-${value++}"
    map.coSet(key, v)
    logger.debug { "Set $key=$v" }
  }
}

private fun CoroutineScope.launchCollectors(
  hazelcast: HazelcastInstance,
  mapName: String,
  keys: List<String>,
  inputs: TestCase,
) {
  keys.forEach { id ->
    (1..inputs.readersPerKey).forEach { num ->
      launch { watchKey(hazelcast, mapName, id, num, inputs.filterStrategy) }
    }
  }
}

private suspend fun watchKey(
  hazelcast: HazelcastInstance,
  mapName: String,
  id: String,
  collectorNumber: Int,
  position: FilterStrategy,
) {
  val pipeline = Pipeline.create()
  pipeline.readFrom(journalSource(mapName, id, position)).withoutTimestamps()
    .setName("source-$collectorNumber-$id")
    .let { stage ->
      if (position == FilterStrategy.JetStream) stage.filter(StreamPredicate(id))
      else stage
    }
    .writeTo(Sinks.logger { "Collector $collectorNumber for $id received $it" })
    .setName("sink-$collectorNumber-$id")
  val job = hazelcast.jet.newJob(pipeline)
  job.await()
}
