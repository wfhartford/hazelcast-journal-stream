package ca.cutterslade.hz.stream

import com.hazelcast.config.Config
import com.hazelcast.config.EventJournalConfig
import com.hazelcast.config.MapConfig
import com.hazelcast.core.EntryEventType
import com.hazelcast.function.FunctionEx
import com.hazelcast.function.PredicateEx
import com.hazelcast.jet.Job
import com.hazelcast.jet.pipeline.JournalInitialPosition
import com.hazelcast.jet.pipeline.Sources
import com.hazelcast.map.EventJournalMapEvent
import com.hazelcast.map.IMap
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.future.await
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

fun hazelcastConfig(): Config =
  Config().apply {
    properties["hazelcast.logging.type"] = "slf4j"
    jetConfig.isEnabled = true
    addMapConfig(
      MapConfig().apply {
        name = "default"
        eventJournalConfig = EventJournalConfig().apply {
          isEnabled = true
          capacity = 100_000
        }
      }
    )
  }

suspend fun <K : Any, V : Any> IMap<K, V>.coSet(key: K, value: V) {
  setAsync(key, value).await()
}

suspend fun Job.await() {
  suspendCancellableCoroutine { cont: CancellableContinuation<Unit> ->
    future.whenComplete { _, t ->
      if (t == null) cont.resume(Unit)
      else cont.resumeWithException(t)
    }
    cont.invokeOnCancellation { cancel() }
  }
}

enum class FilterStrategy {
  JournalPredicate,
  JournalProjection,
  JetStream,
}

fun journalSource(
  mapName: String,
  id: String,
  filterStrategy: FilterStrategy,
) = Sources.mapJournal(
  mapName,
  JournalInitialPosition.START_FROM_CURRENT,
  if (filterStrategy == FilterStrategy.JournalProjection) JournalProjectionFilterFunction(id) else JournalProjectionAllFunction,
  if (filterStrategy == FilterStrategy.JournalPredicate) JournalPredicate(id) else PredicateEx.alwaysTrue(),
)

object JournalProjectionAllFunction : FunctionEx<EventJournalMapEvent<String, String>, Pair<String, String>?> {
  override fun applyEx(event: EventJournalMapEvent<String, String>): Pair<String, String>? =
    event.takeIf { it.type == EntryEventType.ADDED || it.type == EntryEventType.UPDATED }
      ?.let { it.key to it.newValue }
}

class JournalProjectionFilterFunction(private val key: String) :
  FunctionEx<EventJournalMapEvent<String, String>, Pair<String, String>?> {
  override fun applyEx(event: EventJournalMapEvent<String, String>): Pair<String, String>? =
    event.takeIf { it.type == EntryEventType.ADDED || it.type == EntryEventType.UPDATED }
      ?.takeIf { it.key == key }
      ?.let { it.key to it.newValue }
}

class JournalPredicate(private val id: String) : PredicateEx<EventJournalMapEvent<String, String>> {
  override fun testEx(event: EventJournalMapEvent<String, String>): Boolean =
    (event.type == EntryEventType.ADDED || event.type == EntryEventType.UPDATED) && event.key == id
}

class StreamPredicate(private val id: String) : PredicateEx<Pair<String, String>?> {
  override fun testEx(event: Pair<String, String>?): Boolean = event?.first == id
}
