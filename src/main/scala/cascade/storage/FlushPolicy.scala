package cascade.storage

enum FlushPolicy:
  /** Acknowledge after append and flush dirty segments asynchronously by time or size. */
  case Periodic

  /** Force every append to stable storage before returning. Intended for strict single-node durability. */
  case Sync

object FlushPolicy:
  def parse(value: String): FlushPolicy = value.toLowerCase(java.util.Locale.ROOT) match
    case "periodic" => Periodic
    case "sync"     => Sync
    case _          => throw IllegalArgumentException(s"unknown flush policy: $value (expected periodic or sync)")
