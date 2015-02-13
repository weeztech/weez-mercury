package com.weez.mercury.common

trait AsyncDequeue[A] {
  def dequeue(f: QueueState[A] => Unit): Unit
}

trait AsyncQueue[A, B] extends AsyncDequeue[B] {
  @inline final def isEnd = endState.isDefined

  def endState: Option[QueueEnd]

  def upstream: Option[AsyncDequeue[A]]

  def enqueue(buf: QueueState[A]): Unit

  def enqueue(buf: A): Unit = enqueue(QueueItem(buf))

  def end(): Unit = enqueue(QueueFinish)

  def <<(upstream: AsyncDequeue[A]): Unit

  def >>[C](downstream: AsyncQueue[B, C]): AsyncQueue[A, C] =
    new AsyncQueue.PipedQueue(this, downstream)
}

sealed trait QueueState[+A]

case class QueueItem[A](item: A) extends QueueState[A]

sealed trait QueueEnd extends QueueState[Nothing]

case class QueueError(ex: Throwable) extends QueueEnd

case object QueueFinish extends QueueEnd

object AsyncQueue {

  import scala.concurrent._
  import scala.util.control._

  /**
   * 注意：非线程安全对象
   */
  trait AbstractQueue[A, B] extends AsyncQueue[A, B] {
    self =>
    private var receive: QueueState[B] => Unit = null
    var endState: Option[QueueEnd] = None
    var upstream: Option[AsyncDequeue[A]] = None

    protected def enqueue0(buf: A)

    protected def dequeue0(): B

    def pending = receive != null

    def dequeue(f: QueueState[B] => Unit) = {
      require(receive == null)
      receive = f
      processPending()
    }

    def enqueue(buf: QueueState[A]) = {
      require(endState.isEmpty)
      buf match {
        case x: QueueItem[A] =>
          try {
            enqueue0(x.item)
          } catch {
            case NonFatal(ex) =>
              endState = Some(QueueError(ex))
          }
        case x: QueueEnd => endState = Some(x)
      }
      processPending()
    }

    private def processPending(): Unit = {
      if (receive != null) {
        endState match {
          case Some(x) => received(x)
          case None =>
            try {
              received(QueueItem(dequeue0()))
            } catch {
              case NotReadyException =>
                upstream foreach (_ dequeue enqueue)
              case NonFatal(ex) =>
                endState = Some(QueueError(ex))
                received(endState.get)
            }
        }
      }
    }

    protected def received(x: QueueState[B]) = {
      try {
        receive(x)
        receive = null
      } catch {
        case NonFatal(ex) =>
          if (endState.isEmpty)
            endState = Some(QueueError(ex))
      }
    }

    def <<(up: AsyncDequeue[A]) = {
      upstream = Some(up)
    }

    protected def notReady() = throw NotReadyException
  }

  trait ThreadSafeQueue[A, B] extends AbstractQueue[A, B] {
    implicit protected val executor: ExecutionContext

    override def enqueue(buf: QueueState[A]) = this.synchronized {
      super.enqueue(buf)
    }

    override def dequeue(f: QueueState[B] => Unit) = this.synchronized {
      super.dequeue(f)
    }

    override protected def received(x: QueueState[B]) = {
      Future {
        this.synchronized {
          super.received(x)
        }
      }
    }
  }

  /**
   * 线程安全的，无缓冲的，队列实现。
   * enqueue方法会阻塞直到dequeue被执行。
   */
  class SwapBuffer[A](implicit executor: ExecutionContext) extends AbstractQueue[A, A] {
    private var swap: A = _
    private var hasValue = false

    protected def enqueue0(buf: A) = {
      swap = buf
      hasValue = true
      if (!pending) {
        while (hasValue) this.wait()
      }
    }

    protected def dequeue0(): A = {
      if (hasValue) {
        this.notify()
        val buf = swap
        hasValue = false
        swap = null.asInstanceOf[A]
        buf
      } else
        notReady()
    }
  }


  class BufferedQueue[A](minBufferLength: Int) extends AbstractQueue[A, A] {
    private val queue = scala.collection.mutable.Queue[A]()

    protected def enqueue0(buf: A) = {
      queue.enqueue(buf)
    }

    protected def dequeue0(): A = {
      if (queue.isEmpty) notReady()
      val buf = queue.dequeue()
      fill()
      buf
    }

    private def fill(): Unit = {
      if (queue.length < minBufferLength) {
        upstream foreach { x =>
          x dequeue { buf =>
            enqueue(buf)
            fill()
          }
        }
      }
    }

    override def <<(up: AsyncDequeue[A]) = {
      super.<<(up)
      fill()
    }
  }

  class PipedQueue[A, B](first: AsyncQueue[A, _], last: AsyncQueue[_, B]) extends AsyncQueue[A, B] {
    var endState: Option[QueueEnd] = None

    def upstream = first.upstream

    def enqueue(buf: QueueState[A]) = first.enqueue(buf)

    def dequeue(f: QueueState[B] => Unit) = {
      last.dequeue { buf =>
        buf match {
          case x: QueueEnd => endState = Some(x)
          case _ =>
        }
        f(buf)
      }
    }

    def <<(upstream: AsyncDequeue[A]) = first << upstream

    override def >>[C](downstream: AsyncQueue[B, C]): AsyncQueue[A, C] = {
      downstream << last
      new PipedQueue(first, downstream)
    }
  }

  case object NotReadyException extends Exception with NoStackTrace

}