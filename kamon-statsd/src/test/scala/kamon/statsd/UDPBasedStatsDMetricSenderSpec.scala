/*
 * =========================================================================================
 * Copyright © 2013-2015 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.statsd

import java.net.{ DatagramSocket, InetSocketAddress }
import java.nio.charset.Charset

import akka.actor.Props
import akka.testkit.TestProbe
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.channel.{ Channel, ChannelHandlerContext, SimpleChannelInboundHandler }
import kamon.Kamon
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot
import kamon.metric._
import kamon.metric.instrument.{ InstrumentFactory, UnitOfMeasurement }
import kamon.testkit.BaseKamonSpec
import kamon.util.MilliTimestamp

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

abstract class UDPBasedStatsDMetricSenderSpec(actorSystemName: String) extends BaseKamonSpec(actorSystemName) {

  implicit val metricKeyGenerator = new SimpleMetricKeyGenerator(system.settings.config) {
    override def hostName: String = "localhost_local"
  }

  lazy val port = {
    val socket = new DatagramSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  val statsDConfig = config.getConfig("kamon.statsd")

  trait UdpListenerFixture {
    val testEntity = Entity("user/kamon", "test")

    def buildMetricKey(entity: Entity, metricName: String)(implicit metricKeyGenerator: SimpleMetricKeyGenerator): String = {
      val metricKey = HistogramKey(metricName, UnitOfMeasurement.Unknown)
      metricKeyGenerator.generateKey(entity, metricKey)
    }

    def buildRecorder(name: String): TestEntityRecorder =
      Kamon.metrics.entity(TestEntityRecorder, name)

    def newSender(syncProbe: TestProbe): Props

    def setup(metrics: Map[Entity, EntitySnapshot]): (Channel, TestProbe) = {

      import Netty._

      val probe = TestProbe()
      val channelFuture: Future[Channel] = new Bootstrap()
        .channel(classOf[NioDatagramChannel])
        .group(new NioEventLoopGroup())
        .handler(new PacketProbeRepeater(probe))
        .bind(new InetSocketAddress(port))

      val channel = Await.result(channelFuture, 10 seconds)

      val metricsSender = system.actorOf(newSender(probe))

      probe.expectMsgType[Channel]

      val fakeSnapshot = TickMetricSnapshot(MilliTimestamp.now, MilliTimestamp.now, metrics)
      metricsSender ! fakeSnapshot
      (channel, probe)
    }

    def teardown(channel: Channel) = {

      import Netty._

      val channelFuture: Future[Channel] = channel.close()
      Await.ready(channelFuture, 10 seconds)
    }

    def expectUDPPacket(expected: String, probe: TestProbe): Unit = {
      val packet = probe.expectMsgType[String]
      packet should be(expected)
    }

    def doWithChannel(metrics: Map[Entity, EntitySnapshot], doWithChannel: (TestProbe) ⇒ Unit): Unit = {
      val (channel, probe) = setup(metrics: Map[Entity, EntitySnapshot])
      try {
        doWithChannel(probe)
      } finally {
        teardown(channel)
      }
    }
  }

  class PacketProbeRepeater(probe: TestProbe) extends SimpleChannelInboundHandler[DatagramPacket] {

    override def channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket): Unit = {
      val message = msg.content().toString(Charset.defaultCharset())
      probe.ref ! message
    }
  }

  class TestEntityRecorder(instrumentFactory: InstrumentFactory) extends GenericEntityRecorder(instrumentFactory) {
    val metricOne = histogram("metric-one")
    val metricTwo = histogram("metric-two")
  }

  object TestEntityRecorder extends EntityRecorderFactory[TestEntityRecorder] {
    def category: String = "test"

    def createRecorder(instrumentFactory: InstrumentFactory): TestEntityRecorder = new TestEntityRecorder(instrumentFactory)
  }

}

