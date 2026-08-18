package cascade.coordinator

trait CoordinatorCheckpoint:
  /** Commits the combined coordinator image and restores the last committed image on failure. */
  def commit(): Boolean

object CoordinatorCheckpoint:
  val Local: CoordinatorCheckpoint = () => true
