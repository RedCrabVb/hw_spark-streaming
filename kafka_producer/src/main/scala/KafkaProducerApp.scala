import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import scala.util.Random
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

//Customer. the amount in the check. shopping list
case class Customer(id: Int, amount: Int, shoppingList: List[String])


object KafkaProducerApp extends App {
  implicit val formats = DefaultFormats

  val log = Logger.getLogger("KafkaProducerApp")

  val brokers = "localhost:9092"
  val topic = "records"

  private val properties = {
    val properties = new Properties()

    properties.put(BOOTSTRAP_SERVERS_CONFIG, brokers)
    properties.put(ACKS_CONFIG, "all")
    properties.put(RETRIES_CONFIG, 0)
    properties.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    properties
  }

  val producer = new KafkaProducer[Long, String](properties)

  var key = 0L
  val shoppingList = "Water.Sweet.Bread.Meat.Groats.Fish.Vegetables".split("\\.").toList

  while (true) {
    val timestamp = Instant.now.toEpochMilli
    key += 1

    val currentShoppingList = Random.shuffle(shoppingList).take(Random.nextInt(3) + 1)
    val sum = (Random.nextInt(90) + 10) * currentShoppingList.size
    val id = Random.nextInt(9999)

    val value: String = write(Customer(id, sum, currentShoppingList))
    val record = new ProducerRecord(topic, null, timestamp, key, value)

    producer.send(record)

    log.info(s"$record")

    TimeUnit.SECONDS.sleep(Random.nextInt(3) + 1)
  }

}
