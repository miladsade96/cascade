package cascade.coordinator

import munit.FunSuite

final class ShardEncodingCacheSuite extends FunSuite:
  test("unchanged shards reuse immutable payloads and changed shards re-encode") {
    val cache = ShardEncodingCache[Int](3, value => Vector(value.toByte))
    val first = cache.capture(Vector(1, 2, 3))
    val second = cache.capture(Vector(1, 4, 3))
    assertEquals((first.encoded, first.reused, first.encodedBytes), (3, 0, 3L))
    assertEquals((second.encoded, second.reused, second.encodedBytes), (1, 2, 1L))
    assert(first.payloads(0) eq second.payloads(0))
    assertEquals(first.payloads, Vector(Vector[Byte](1), Vector[Byte](2), Vector[Byte](3)))
    assertEquals(cache.capture(Vector(1, 4, 3)).encoded, 0)
  }

  test("failed encodes cannot partially replace the previous cache") {
    val cache = ShardEncodingCache[Int](2, value =>
      require(value >= 0, "negative input")
      Vector(value.toByte)
    )
    val original = cache.capture(Vector(1, 2))
    intercept[IllegalArgumentException](cache.capture(Vector(3, -1)))
    val restored = cache.capture(Vector(1, 2))
    assertEquals(restored.encoded, 0)
    assert(original.payloads(0) eq restored.payloads(0))
    assertEquals(cache.capture(Vector(3, 4)).encoded, 2)
  }

  test("invalid layouts fail without changing a valid cache") {
    intercept[IllegalArgumentException](ShardEncodingCache[Int](0, _ => Vector.empty))
    intercept[IllegalArgumentException](ShardEncodingCache[Int](130, _ => Vector.empty))
    val cache = ShardEncodingCache[Int](1, _.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8).toVector)
    cache.capture(Vector(1))
    intercept[IllegalArgumentException](cache.capture(Vector(2, 3)))
    assertEquals(cache.capture(Vector(1)).reused, 1)
  }

  test("equal hash codes never substitute for immutable content equality") {
    final case class Collision(value: Int):
      override def hashCode(): Int = 0
    val cache = ShardEncodingCache[Collision](1, value => Vector(value.value.toByte))
    assertEquals(cache.capture(Vector(Collision(1))).payloads, Vector(Vector[Byte](1)))
    assertEquals(cache.capture(Vector(Collision(2))).payloads, Vector(Vector[Byte](2)))
    assertEquals(cache.capture(Vector(Collision(1))).encoded, 1)
  }
