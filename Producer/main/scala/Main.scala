import Main.record

object Main extends App {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  import scala.util.Random

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="test4"
  def dataCreation(n : Int, scenario : Int){
    val tempMax = 37.5
    val tempMin = 36.5
    val phMax = 7.35
    val phMin = 7.45
    val glucoseMax = 0.65
    val glucoseMin = 1.10
    val cholesterolMax = 0.4
    val cholesterolMin = 1.5
    val heartbeatMax = 60
    val heartbeatMin = 120
    val batteryMax = 100
    val batteryMin = 11

    val r = new Random()

    var temp = tempMin + r.nextFloat() * (tempMax - tempMin)
    var ph = phMin + r.nextFloat() * (phMax - phMin)
    var glucose = glucoseMin + r.nextFloat() * (glucoseMax - glucoseMin)
    var cholesterol = cholesterolMin + r.nextFloat() * (cholesterolMax - cholesterolMin)
    var heartbeat = heartbeatMin + r.nextFloat() * (heartbeatMax - heartbeatMin)
    var battery = batteryMin + r.nextFloat() * (batteryMax - batteryMin)

    if(scenario == 1 && n == 0) {
      battery = 7
      heartbeat = 180
    }
    else if(scenario == 2 && n == 0){
      battery = 5
      ph = 7.6
    }

    val record = new ProducerRecord(TOPIC, "key", "{\"temp\" :  " + temp + ",\"ph\" : "
      + ph + ",\"glucose\" : " + glucose + ",\"cholesterol\" : "
      + cholesterol + ",\"heartbeat\" : " + heartbeat +", \"battery\" : " + battery + "}")
    producer.send(record)

    if(n>0) {
      dataCreation(n - 1,scenario)
    }
  }

  dataCreation(5,0)

  val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
  producer.send(record)

  producer.close()
}