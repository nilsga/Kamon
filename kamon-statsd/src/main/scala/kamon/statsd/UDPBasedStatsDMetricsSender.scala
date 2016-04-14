/*
 * =========================================================================================
 * Copyright © 2013-2015 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.statsd

import java.net.InetSocketAddress
import java.text.{ DecimalFormat, DecimalFormatSymbols }
import java.util.Locale

import akka.actor.Actor
import akka.pattern._
import com.typesafe.config.Config
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot
import kamon.statsd.Netty._

import scala.concurrent.{ Future, Promise }

trait StatsDValueFormatters {

  val symbols = DecimalFormatSymbols.getInstance(Locale.US)
  symbols.setDecimalSeparator('.')
  // Just in case there is some weird locale config we are not aware of.

  // Absurdly high number of decimal digits, let the other end lose precision if it needs to.
  val samplingRateFormat = new DecimalFormat("#.################################################################", symbols)

  def encodeStatsDTimer(level: Long, count: Long): String = {
    val samplingRate: Double = 1D / count
    level.toString + "|ms" + (if (samplingRate != 1D) "|@" + samplingRateFormat.format(samplingRate) else "")
  }

  def encodeStatsDCounter(count: Long): String = count.toString + "|c"
}

/**
 * Base class for different StatsD senders utilizing UDP protocol. It implies use of one statsd server.
 * @param statsDConfig Config to read settings specific to this sender
 * @param metricKeyGenerator Key generator for all metrics sent by this sender
 */
abstract class UDPBasedStatsDMetricsSender(statsDConfig: Config, metricKeyGenerator: MetricKeyGenerator)
    extends Actor with StatsDValueFormatters {

  import context.dispatcher

  val statsDHost = statsDConfig.getString("hostname")
  val statsDPort = statsDConfig.getInt("port")
  lazy val socketAddress = new InetSocketAddress(statsDHost, statsDPort)

  self ! InitializeChannel

  def receive = {
    case InitializeChannel ⇒
      new Bootstrap().group(new NioEventLoopGroup()).channel(classOf[NioDatagramChannel]).handler(channelHandler)
        .bind(new InetSocketAddress(0)).map(ChannelReady(_)) pipeTo self
    case ChannelReady(channel) ⇒
      context.become(ready(channel))
  }

  def ready(channel: Channel): Receive = {
    case tick: TickMetricSnapshot ⇒
      writeMetricsToRemote(tick,
        (data: String) ⇒ {
          val buff = channel.alloc().buffer(data.length).writeBytes(data.getBytes)
          channel.writeAndFlush(new DatagramPacket(buff, socketAddress))
        })
  }

  def writeMetricsToRemote(tick: TickMetricSnapshot, flushToUDP: String ⇒ Unit): Unit

}

object Netty {
  case object InitializeChannel
  case class ChannelReady(channel: Channel)

  implicit lazy val channelHandler = new ChannelInitializer[Channel] {
    override def initChannel(ch: Channel): Unit = {}
  }

  implicit def toFuture(channelFuture: ChannelFuture): Future[Channel] = {
    val promise = Promise[Channel]()
    channelFuture.addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess) promise.success(future.channel())
        else promise.failure(future.cause())
      }
    })
    promise.future
  }
}
