package cascade.security

import cascade.storage.AtomicFileLifecycle
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption.{CREATE_NEW, TRUNCATE_EXISTING, WRITE}
import java.nio.file.{FileAlreadyExistsException, Files, Path}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.util.control.NonFatal

final class ReloadableAuthorizer(path: Path, superUsers: Set[String], reloadIntervalMillis: Long):
  require(reloadIntervalMillis >= 0L, "ACL reload interval cannot be negative")

  private val normalizedPath = path.toAbsolutePath.normalize()
  ReloadableAuthorizer.initialize(normalizedPath)
  private val authorizer = AtomicReference(AclAuthorizer.load(normalizedPath, superUsers))
  private val nextReloadNanos = AtomicLong(deadlineFromNow())
  private val reloadError = AtomicReference(Option.empty[String])

  def authorize(principal: String, operation: AclOperation, resource: Resource, host: String = "*"): Boolean =
    reloadIfDue()
    authorizer.get().authorize(principal, operation, resource, host)

  def authorizeAny(principals: Set[String], operation: AclOperation, resource: Resource, host: String = "*"): Boolean =
    reloadIfDue()
    authorizer.get().authorizeAny(principals, operation, resource, host)

  def rules: Vector[AclRule] =
    reloadIfDue()
    authorizer.get().rules

  def createRules(additions: Vector[AclRule]): Either[String, Unit] = mutate { current =>
    current ++ additions.filterNot(current.contains)
  }

  def deleteRules(filter: AclFilter): Either[String, Vector[AclRule]] = synchronized {
    try
      val current = AclAuthorizer.parse(normalizedPath)
      val removed = current.filter(_.matchesFilter(filter))
      persist(current.filterNot(removed.contains))
      Right(removed)
    catch case NonFatal(error) => Left(message(error))
  }

  def lastReloadError: Option[String] = reloadError.get()

  def reloadNow(): Boolean = synchronized {
    try
      replaceSnapshot(AclAuthorizer.parse(normalizedPath))
      true
    catch
      case NonFatal(error) =>
        reloadError.set(Some(message(error)))
        nextReloadNanos.set(deadlineFromNow())
        false
  }

  private def mutate(update: Vector[AclRule] => Vector[AclRule]): Either[String, Unit] = synchronized {
    try
      persist(update(AclAuthorizer.parse(normalizedPath)))
      Right(())
    catch case NonFatal(error) => Left(message(error))
  }

  private def persist(rules: Vector[AclRule]): Unit =
    val parent = Option(normalizedPath.getParent).getOrElse(throw IllegalArgumentException("ACL file must have a parent directory"))
    Files.createDirectories(parent)
    val temporary = Files.createTempFile(parent, normalizedPath.getFileName.toString + ".", ".tmp")
    try
      val encoded = AclAuthorizer.render(rules).getBytes(StandardCharsets.UTF_8)
      val channel = FileChannel.open(temporary, WRITE, TRUNCATE_EXISTING)
      try
        val buffer = ByteBuffer.wrap(encoded)
        while buffer.hasRemaining do channel.write(buffer): Unit
        channel.force(true)
      finally channel.close()
      AtomicFileLifecycle.replace(temporary, normalizedPath)
      replaceSnapshot(rules)
    finally Files.deleteIfExists(temporary): Unit

  private def replaceSnapshot(rules: Vector[AclRule]): Unit =
    authorizer.set(AclAuthorizer.fromRules(rules, superUsers))
    reloadError.set(None)
    nextReloadNanos.set(deadlineFromNow())

  private def reloadIfDue(): Unit =
    val now = System.nanoTime()
    val deadline = nextReloadNanos.get()
    if now >= deadline && nextReloadNanos.compareAndSet(deadline, Long.MaxValue) then reloadNow(): Unit

  private def deadlineFromNow(): Long =
    val intervalNanos = reloadIntervalMillis * 1_000_000L
    val now = System.nanoTime()
    if Long.MaxValue - now < intervalNanos then Long.MaxValue else now + intervalNanos

  private def message(error: Throwable): String = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)

object ReloadableAuthorizer:
  def apply(path: Path, superUsers: Set[String], reloadIntervalMillis: Long): ReloadableAuthorizer =
    new ReloadableAuthorizer(path, superUsers, reloadIntervalMillis)

  def initialize(path: Path): Unit =
    val parent = Option(path.getParent).getOrElse(throw IllegalArgumentException("ACL file must have a parent directory"))
    Files.createDirectories(parent)
    if !Files.exists(path) then
      try
        val channel = FileChannel.open(path, CREATE_NEW, WRITE)
        try channel.force(true)
        finally channel.close()
      catch case _: FileAlreadyExistsException => ()
