import org.apache.commons.mail._
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import java.time.Duration



object Main extends App {

  import java.util.Properties

  val TOPIC = "alert"

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  def send_email(): Unit ={
    val email = new SimpleEmail()
    email.setHostName("smtp.gmail.com")
    email.setSmtpPort(465)
    email.setAuthenticator(new DefaultAuthenticator("jojo.baskar@gmail.c ", "samjonson"))
    email.setSSLOnConnect(true)
    email.setFrom("jojo.baskar@gmail.com")
    email.setSubject("Alert on the pacemaker battery")
    email.setMsg("The pacemaker battery is under 15% ! There is a high risk of working interruption.")
    email.addTo("romain.legoas@gmail.com")
    email.send()

  }

  while (true) {
    val records = consumer.poll(Duration.ofMillis(1000))
    for (record <- records.asScala) {
      send_email()
    }
  }
}

