package com.redislabs.provider.redis.rdd

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, FunSuite}
import scala.io.Source.fromInputStream
import com.redislabs.provider.redis._

class RedisRDDStandaloneSuite extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {

  override def beforeAll() {
    super.beforeAll()

    sc = new SparkContext(new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "127.0.0.1")
      .set("redis.port", "6379")
      .set("redis.auth", "passwd")
    )
    sc.setLogLevel("WARN")
    content = fromInputStream(getClass.getClassLoader.getResourceAsStream("blog")).
      getLines.toArray.mkString("\n")

    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toString))

    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))

    redisConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379, "passwd"))

    // Flush all the hosts
    redisConfig.hosts.foreach( node => {
      val conn = node.connect
      conn.flushAll
      conn.close
    })

    sc.toRedisKV(wcnts)(redisConfig)
    sc.toRedisZSET(wcnts, "all:words:cnt:sortedset")(redisConfig)
    sc.toRedisHASH(wcnts, "all:words:cnt:hash" )(redisConfig)
    sc.toRedisLIST(wds, "all:words:list" )(redisConfig)
    sc.toRedisSET(wds, "all:words:set")(redisConfig)


    val testRDD = sc.parallelize(Seq(("life", "la_vie"), ("is", "est"), ("beautiful", "belle")))
    sc.toRedisHASH(testRDD, "test:hash:standalone")
  }

  test("RedisKVRDD - default(standalone)") {
    val redisKVRDD = sc.fromRedisKV("*")
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisKVRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisKVRDD = sc.fromRedisKV("*")
    val kvContents = redisKVRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    kvContents should be (wcnts)
  }

  test("RedisZsetRDD - default(standalone)") {
    val redisZSetWithScore = sc.fromRedisZSetWithScore("all:words:cnt:sortedset")
    val zsetWithScore = redisZSetWithScore.sortByKey().collect

    val redisZSet = sc.fromRedisZSet("all:words:cnt:sortedset")
    val zset = redisZSet.collect.sorted

    val redisZRangeWithScore = sc.fromRedisZRangeWithScore("all:words:cnt:sortedset", 0, 15)
    val zrangeWithScore = redisZRangeWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRange = sc.fromRedisZRange("all:words:cnt:sortedset", 0, 15)
    val zrange = redisZRange.collect.sorted

    val redisZRangeByScoreWithScore = sc.fromRedisZRangeByScoreWithScore("all:words:cnt:sortedset", 3, 9)
    val zrangeByScoreWithScore = redisZRangeByScoreWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRangeByScore = sc.fromRedisZRangeByScore("all:words:cnt:sortedset", 3, 9)
    val zrangeByScore = redisZRangeByScore.collect.sorted

    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toDouble))

    zsetWithScore should be (wcnts.toArray.sortBy(_._1))
    zset should be (wcnts.map(_._1).toArray.sorted)
    zrangeWithScore should be (wcnts.toArray.sortBy(x => (x._2, x._1)).take(16))
    zrange should be (wcnts.toArray.sortBy(x => (x._2, x._1)).take(16).map(_._1))
    zrangeByScoreWithScore should be (wcnts.toArray.filter(x => (x._2 >= 3 && x._2 <= 9)).sortBy(x => (x._2, x._1)))
    zrangeByScore should be (wcnts.toArray.filter(x => (x._2 >= 3 && x._2 <= 9)).map(_._1).sorted)
  }

  test("RedisZsetRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisZSetWithScore = sc.fromRedisZSetWithScore("all:words:cnt:sortedset")
    val zsetWithScore = redisZSetWithScore.sortByKey().collect

    val redisZSet = sc.fromRedisZSet("all:words:cnt:sortedset")
    val zset = redisZSet.collect.sorted

    val redisZRangeWithScore = sc.fromRedisZRangeWithScore("all:words:cnt:sortedset", 0, 15)
    val zrangeWithScore = redisZRangeWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRange = sc.fromRedisZRange("all:words:cnt:sortedset", 0, 15)
    val zrange = redisZRange.collect.sorted

    val redisZRangeByScoreWithScore = sc.fromRedisZRangeByScoreWithScore("all:words:cnt:sortedset", 3, 9)
    val zrangeByScoreWithScore = redisZRangeByScoreWithScore.collect.sortBy(x => (x._2, x._1))

    val redisZRangeByScore = sc.fromRedisZRangeByScore("all:words:cnt:sortedset", 3, 9)
    val zrangeByScore = redisZRangeByScore.collect.sorted

    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toDouble))

    zsetWithScore should be (wcnts.toArray.sortBy(_._1))
    zset should be (wcnts.map(_._1).toArray.sorted)
    zrangeWithScore should be (wcnts.toArray.sortBy(x => (x._2, x._1)).take(16))
    zrange should be (wcnts.toArray.sortBy(x => (x._2, x._1)).take(16).map(_._1))
    zrangeByScoreWithScore should be (wcnts.toArray.filter(x => (x._2 >= 3 && x._2 <= 9)).sortBy(x => (x._2, x._1)))
    zrangeByScore should be (wcnts.toArray.filter(x => (x._2 >= 3 && x._2 <= 9)).map(_._1).sorted)
  }

  test("RedisHashRDD - default(standalone)") {
    val redisHashRDD = sc.fromRedisHash( "all:words:cnt:hash")
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisHashRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisHashRDD = sc.fromRedisHash( "all:words:cnt:hash")
    val hashContents = redisHashRDD.sortByKey().collect
    val wcnts = content.split("\\W+").filter(!_.isEmpty).map((_, 1)).groupBy(_._1).
      map(x => (x._1, x._2.map(_._2).reduce(_ + _).toString)).toArray.sortBy(_._1)
    hashContents should be (wcnts)
  }

  test("RedisHashRDD - default(standalone) - hget: Key & field exists") {
    val value = sc.redisHGET("test:hash:standalone", "beautiful")
    value should be ("belle")
  }

  test("RedisHashRDD - standalone - hget: Key & field exists") {
    implicit val c: RedisConfig = redisConfig
    val value = sc.redisHGET("test:hash:standalone", "beautiful")
    value should be ("belle")
  }

  test("RedisHashRDD - default(standalone) - hget: Key does not exist") {
    val value = sc.redisHGET("test:hash:null", "beautiful")
    value should be (null)
  }

  test("RedisHashRDD - standalone - hget: Key does not exist") {
    implicit val c: RedisConfig = redisConfig
    val value = sc.redisHGET("test:hash:null", "beautiful")
    value should be (null)
  }

  test("RedisHashRDD - default(standalone) - hget: Field does not exist") {
    val value = sc.redisHGET("test:hash:standalone", "difficult")
    value should be (null)
  }

  test("RedisHashRDD - standalone - hget: Field does not exist") {
    implicit val c: RedisConfig = redisConfig
    val value = sc.redisHGET("test:hash:standalone", "difficult")
    value should be (null)
  }

  test("RedisHashRDD - default(standalone) - hmget") {
    val fieldRDD = sc.parallelize("life is difficult".split(" "))
    val values = sc.redisHMGET("test:hash:standalone", fieldRDD).collect.sorted

    values should be (Array(("life", "la_vie"), ("is", "est"), ("difficult", null)).sortBy(_._1))
  }

  test("RedisHashRDD - standalone - hmget") {
    implicit val c: RedisConfig = redisConfig
    val fieldRDD = sc.parallelize("life is difficult".split(" "))
    val values = sc.redisHMGET("test:hash:standalone", fieldRDD).collect.sorted

    values should be (Array(("life", "la_vie"), ("is", "est"), ("difficult", null)).sortBy(_._1))
  }

  test("RedisHashRDD - default(standalone) - hmget: key does not exist") {

    val fieldRDD = sc.parallelize("life is difficult".split(" "))
    val values = sc.redisHMGET("test:hash:null", fieldRDD).collect.sorted

    values should be (Array(("life", null), ("is", null), ("difficult", null)).sortBy(_._1))
  }

  test("RedisHashRDD - standalone - hmget: key does not exist") {
    implicit val c: RedisConfig = redisConfig
    val fieldRDD = sc.parallelize("life is difficult".split(" "))
    val values = sc.redisHMGET("test:hash:null", fieldRDD).collect.sorted

    values should be (Array(("life", null), ("is", null), ("difficult", null)).sortBy(_._1))
  }

  test("RedisHashRDD - default(standalone) - hmget in parallel") {
    val fieldRDD = sc.parallelize("life is difficult".split(" "))
    val values = sc.redisHMGETParallel("test:hash:standalone", fieldRDD).collect.sorted

    values should be (Array(("life", "la_vie"), ("is", "est"), ("difficult", null)).sortBy(_._1))
  }

  test("RedisHashRDD - standalone - hmget in parallel") {
    implicit val c: RedisConfig = redisConfig
    val fieldRDD = sc.parallelize("life is difficult".split(" "))
    val values = sc.redisHMGETParallel("test:hash:standalone", fieldRDD).collect.sorted

    values should be (Array(("life", "la_vie"), ("is", "est"), ("difficult", null)).sortBy(_._1))
  }

  test("RedisHashRDD - default(standalone) - hmget in parallel: key does not exist") {

    val fieldRDD = sc.parallelize("life is difficult".split(" "))
    val values = sc.redisHMGETParallel("test:hash:null", fieldRDD).collect.sorted

    values should be (Array(("life", null), ("is", null), ("difficult", null)).sortBy(_._1))
  }

  test("RedisHashRDD - standalone - hmget in parallel: key does not exist") {
    implicit val c: RedisConfig = redisConfig
    val fieldRDD = sc.parallelize("life is difficult".split(" "))
    val values = sc.redisHMGETParallel("test:hash:null", fieldRDD).collect.sorted

    values should be (Array(("life", null), ("is", null), ("difficult", null)).sortBy(_._1))
  }

  test("RedisHashRDD - default(standalone) - Remove fields from hash") {
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toInt))
    val wcntsToRemove = wcnts.filter(_._2 < 4)
    val fieldsToRemove = wcntsToRemove.map(_._1)
    sc.redisHDEL("all:words:cnt:hash", fieldsToRemove)

    val redisHashRDD = sc.fromRedisHash("all:words:cnt:hash")
    val hashContents = redisHashRDD.map(x => (x._1, x._2.toInt)).sortByKey().collect
    val filteredWcnts = wcnts.subtract(wcntsToRemove).sortByKey().collect
    hashContents should be (filteredWcnts)
  }

  test("RedisHashRDD - standalone - Remove fields from hash") {
    implicit val c: RedisConfig = redisConfig
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toInt))
    val wcntsToRemove = wcnts.filter(_._2 < 6)
    val fieldsToRemove = wcntsToRemove.map(_._1)
    sc.redisHDEL("all:words:cnt:hash", fieldsToRemove)

    val redisHashRDD = sc.fromRedisHash("all:words:cnt:hash")
    val hashContents = redisHashRDD.map(x => (x._1, x._2.toInt)).sortByKey().collect
    val filteredWcnts = wcnts.subtract(wcntsToRemove).sortByKey().collect
    hashContents should be (filteredWcnts)
  }

  test("RedisListRDD - default(standalone)") {
    val redisListRDD = sc.fromRedisList( "all:words:list")
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisListRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisListRDD = sc.fromRedisList( "all:words:list")
    val listContents = redisListRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).sorted
    listContents should be (ws)
  }

  test("RedisSetRDD - default(standalone)") {
    val redisSetRDD = sc.fromRedisSet( "all:words:set")
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be (ws)
  }

  test("RedisSetRDD - standalone") {
    implicit val c: RedisConfig = redisConfig
    val redisSetRDD = sc.fromRedisSet( "all:words:set")
    val setContents = redisSetRDD.sortBy(x => x).collect
    val ws = content.split("\\W+").filter(!_.isEmpty).distinct.sorted
    setContents should be (ws)
  }

  test("RedisSetRDD - default(standalone) - Remove elements from set") {
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toInt))
    val wordsToRemove = wcnts.filter(_._2 < 4).map(_._1)
    sc.redisSREM(wordsToRemove, "all:words:set")

    val redisSetRDD = sc.fromRedisSet("all:words:set")
    val setContents = redisSetRDD.sortBy(x => x).collect
    val filteredWs = wcnts.map(_._1).subtract(wordsToRemove).sortBy(x => x).collect
    setContents should be (filteredWs)
  }

  test("RedisSetRDD - standalone - Remove elements from set") {
    implicit val c: RedisConfig = redisConfig
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (x._1, x._2.toInt))
    val wordsToRemove = wcnts.filter(_._2 < 6).map(_._1)
    sc.redisSREM(wordsToRemove, "all:words:set")

    val redisSetRDD = sc.fromRedisSet("all:words:set")
    val setContents = redisSetRDD.sortBy(x => x).collect
    val filteredWs = wcnts.map(_._1).subtract(wordsToRemove).sortBy(x => x).collect
    setContents should be (filteredWs)
  }

  test("Expire - default(standalone)") {
    val expireTime = 1
    val prefix = s"#expire in ${expireTime}#:"
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (prefix + x._1, x._2.toString))
    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))
    sc.toRedisKV(wcnts, expireTime)
    sc.toRedisZSET(wcnts, prefix + "all:words:cnt:sortedset", expireTime)
    sc.toRedisHASH(wcnts, prefix + "all:words:cnt:hash", expireTime)
    sc.toRedisLIST(wds, prefix + "all:words:list", expireTime)
    sc.toRedisSET(wds, prefix + "all:words:set", expireTime)
    Thread.sleep(expireTime * 1000 + 1)
    sc.fromRedisKeyPattern(prefix + "*").count should be (0)
  }

  test("Expire - standalone") {
    val expireTime = 1
    val prefix = s"#expire in ${expireTime}#:"
    implicit val c: RedisConfig = redisConfig
    val wcnts = sc.parallelize(content.split("\\W+").filter(!_.isEmpty)).map((_, 1)).
      reduceByKey(_ + _).map(x => (prefix + x._1, x._2.toString))
    val wds = sc.parallelize(content.split("\\W+").filter(!_.isEmpty))
    sc.toRedisKV(wcnts, expireTime)
    sc.toRedisZSET(wcnts, prefix + "all:words:cnt:sortedset", expireTime)
    sc.toRedisHASH(wcnts, prefix + "all:words:cnt:hash", expireTime)
    sc.toRedisLIST(wds, prefix + "all:words:list", expireTime)
    sc.toRedisSET(wds, prefix + "all:words:set", expireTime)
    Thread.sleep(expireTime * 1000 + 1)
    sc.fromRedisKeyPattern(prefix + "*").count should be (0)
  }

  override def afterAll(): Unit = {
    sc.stop
    System.clearProperty("spark.driver.port")
  }
}

