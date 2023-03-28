package ca.cutterslade.hz.stream

import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.function.PredicateEx
import com.hazelcast.jet.Job
import com.hazelcast.jet.pipeline.JournalInitialPosition
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jet.pipeline.Sources
import com.hazelcast.jet.pipeline.StreamStage
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

suspend fun main() {
  val hz = Hazelcast.newHazelcastInstance(hazelcastConfig())
  try {
    multipleConsumersWithPredicate(hz, FilterStrategy.JournalPredicate)
  } finally {
    hz.shutdown()
  }
}

private const val mapName = "multiple-consumers-with-predicate-map"
private const val keyOne = "key-1"
private const val keyTwo = "key-2"
private suspend fun multipleConsumersWithPredicate(hz: HazelcastInstance, strategy: FilterStrategy) {
  coroutineScope {
    launch { producer(hz) }
    launch { logKeyValues(hz, keyOne, strategy).await() }
    launch { logKeyValues(hz, keyTwo, strategy).await() }
    delay(1.minutes)
    cancel()
  }
}

private suspend fun producer(hz: HazelcastInstance) {
  val map = hz.getMap<String, String>(mapName)
  var counter = 0L
  while (currentCoroutineContext().isActive) {
    delay(1.seconds)
    val value = "value-${counter++}"
    map.coSet(keyOne, value)
    map.coSet(keyTwo, value)
  }
}

private fun logKeyValues(hz: HazelcastInstance, key: String, strategy: FilterStrategy): Job =
  hz.jet.newJob(strategy.createPipeline(key))

private fun FilterStrategy.createPipeline(key: String): Pipeline {
  val pipe = Pipeline.create()
  val stage = when (this) {
    FilterStrategy.JournalPredicate -> journalPredicateStage(pipe, key)
    FilterStrategy.JournalProjection -> journalProjectionStage(pipe, key)
    FilterStrategy.JetStream -> jetStreamStage(pipe, key)
  }
  stage.writeTo(Sinks.logger())
  return pipe
}

private fun journalPredicateStage(
  pipe: Pipeline,
  key: String
): StreamStage<Pair<String, String>?> =
  pipe.readFrom(
    Sources.mapJournal(
      mapName,
      JournalInitialPosition.START_FROM_CURRENT,
      JournalProjectionAllFunction,
      JournalPredicate(key),
    )
  ).withoutTimestamps()

private fun journalProjectionStage(
  pipe: Pipeline,
  key: String
): StreamStage<Pair<String, String>?> =
  pipe.readFrom(
    Sources.mapJournal(
      mapName,
      JournalInitialPosition.START_FROM_CURRENT,
      JournalProjectionFilterFunction(key),
      PredicateEx.alwaysTrue(),
    )
  ).withoutTimestamps()

private fun jetStreamStage(
  pipe: Pipeline,
  key: String
): StreamStage<Pair<String, String>?> =
  pipe.readFrom(
    Sources.mapJournal(
      mapName,
      JournalInitialPosition.START_FROM_CURRENT,
      JournalProjectionAllFunction,
      PredicateEx.alwaysTrue()
    )
  ).withoutTimestamps()
    .filter(StreamPredicate(key))
