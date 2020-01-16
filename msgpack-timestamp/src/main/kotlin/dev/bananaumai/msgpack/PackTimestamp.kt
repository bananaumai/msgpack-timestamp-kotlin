package dev.bananaumai.msgpack

import java.nio.ByteBuffer
import java.time.Instant
import org.msgpack.core.MessagePacker
import java.util.*

private const val EXT_TIMESTAMP: Byte = -1

fun MessagePacker.packTimestamp(epochMilli: Long): MessagePacker {
    val sec = java.lang.Math.floorDiv(epochMilli, 1000L)
    val nsec = (epochMilli - sec * 1000L) * 1000
    val payload = encodeTimestamp(sec, nsec.toInt())
    return pack(payload)
}

fun MessagePacker.packTimestamp(date: Date): MessagePacker {
    val epochMilli = date.time
    return packTimestamp(epochMilli)
}

fun MessagePacker.packTimestamp(instant: Instant): MessagePacker {
    val payload = encodeTimestamp(instant.epochSecond, instant.nano)
    return pack(payload)
}

private fun MessagePacker.pack(payload: ByteArray): MessagePacker {
    packExtensionTypeHeader(EXT_TIMESTAMP, payload.size)
    writePayload(payload)
    return this
}

internal fun encodeTimestamp(sec: Long, nsec: Int): ByteArray {
    /**
     * Check whether epoch second is within the unsigned 34 bit integer range.
     * If so the data should be encoded as timestamp 32 or timestamp 64 format.
     * Otherwise, it should be encoded as timestamp 96 format.
     */
    if (sec ushr 34 == 0L) {
        /**
         * In []msgpack-java's PR](https://github.com/msgpack/msgpack-java/pull/431) and [official pseudo code](https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type),
         * the predicate to determine that timestamp should be represented as timestamp 32 format or timestamp 64 format is done with following bitwise operation.
         *
         * ```
         * long data64 = (nsec << 34) | sec;
         * if ((data64 & 0xffffffff00000000L) == 0L) {
         *     // sec can be serialized in 32 bits and nsec is 0.
         *     // use timestamp 32
         *     writeTimestamp32((int) sec);
         * } else {
         *     // sec exceeded 32 bits or nsec is not 0.
         *     // use timestamp 64
         *     writeTimestamp64(data64);
         * }
         * ```
         *
         * But in Kotlin, `0xffffffff00000000L` literal expression can not be used due to `value is out of range` error.
         * This is a Kotlin's number literal issues but not solved yet.
         *
         * https://discuss.kotlinlang.org/t/0x80000000-is-not-an-int/123
         * https://youtrack.jetbrains.com/issue/KT-4749?_ga=2.82714645.22005906.1568173346-552942216.1554165274
         *
         * Therefore, following predicate checks nano sec value and sec value as is the comments in reference code.
         */
        if (nsec == 0 && sec <= 0xFFFF_FFFFL) {
            return encodeAsTimestamp32(sec.toInt())
        }

        return encodeAsTimestamp64(sec, nsec)
    }

    return encodeAsTimestamp96(sec, nsec)
}

private fun encodeAsTimestamp32(sec: Int): ByteArray {
    return ByteBuffer.allocate(Int.SIZE_BYTES).putInt(sec).array()
}

private fun encodeAsTimestamp64(sec: Long, nsec: Int): ByteArray {
    val data = (nsec.toLong() shl 34) or sec
    return ByteBuffer.allocate(Long.SIZE_BYTES).putLong(data).array()
}

private fun encodeAsTimestamp96(sec: Long, nsec: Int): ByteArray {
    return ByteBuffer.allocate(Int.SIZE_BYTES + Long.SIZE_BYTES).putInt(nsec).putLong(sec).array()
}
