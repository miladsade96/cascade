package cascade

import cascade.broker.{BrokerConfig, KafkaBroker}
import java.util.concurrent.CountDownLatch

object Main:
  def main(arguments: Array[String]): Unit =
    val config = BrokerConfig.parse(arguments)
    val broker = KafkaBroker(config)
    broker.start()
    Runtime.getRuntime.addShutdownHook(Thread(() => broker.close(), "cascade-shutdown"))
    println(
      s"Cascade broker listening on ${broker.bootstrapServers}; data=${config.dataDirectory.toAbsolutePath}; recovery=${broker.recoveryMode}"
    )
    CountDownLatch(1).await()
