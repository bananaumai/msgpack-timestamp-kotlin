package dev.bananaumai.msgpack

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.junit.Test
import org.msgpack.core.MessagePack

class PackTimestampTest {
    private val formatter = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneOffset.UTC)

    @Test
    fun packTimestamp() {
        /**
         *
         * Structure of expected message pack data
         *
         * +--------+--------+--------+--------+--------+--------+
         * |  0xd6  |   -1   |   seconds in 32-bit unsigned int  |
         * +--------+--------+--------+--------+--------+--------+
         *
         * 0xd6 == (byte) -42
         *
         */
        val expected = ByteBuffer.allocate(1 + 1 + 4).put(-42).put(-1).putInt(1).array()

        /**
         *
         * 1 epoch second stands for 1970-01-01T00:00:01,
         * which will be encoded as 32bit unsigned integer
         *
         */
        val input = Instant.ofEpochSecond(1)

        val packer = MessagePack.newDefaultBufferPacker()
        packer.packTimestamp(input)

        assertThat(packer.toByteArray()).isEqualTo(expected)
    }

    @Test
    fun packTimestamp_withInstantMinAndMax() {
        val inputs = sequenceOf(Instant.MIN, Instant.MAX)

        for (input in inputs) {
            val packer = MessagePack.newDefaultBufferPacker()
            packer.packTimestamp(input)

            val bytes = packer.toByteArray()

            assertWithMessage("ext 8 header")
                .that("%02X".format(bytes[0])).isEqualTo("C7")

            assertWithMessage("payload size(96bit/12byte) header")
                .that("%02X".format(bytes[1])).isEqualTo("0C")

            assertWithMessage("timestampe type header")
                .that("%02X".format(bytes[2])).isEqualTo("FF")

            assertWithMessage("payload size is 12 byte")
                .that(bytes.drop(3).size).isEqualTo(12)
        }
    }

    @Test
    fun encodeTimestamp() {
        data class TestCase(val input: Instant, val expected: ByteArray)
        /**
         *
         * The cases are create in reference to the official spec.
         *
         * https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type
         *
         * Expected test cases are referencing Golang implementation.
         *
         * https://play.golang.org/p/y2L3DKwBNfd
         *
         * This playbook uses the same time encoding procedure with Golang msgpack library.
         *
         * https://github.com/vmihailenco/msgpack/blob/master/time.go#L31
         *
         */
        val testCases = sequenceOf(
            // timestamp 32 patterns
            TestCase(
                ZonedDateTime.parse("1970-01-01T00:00:00", formatter).toInstant(),
                ByteBuffer.allocate(Int.SIZE_BYTES).putInt(0).array() // 0x0000_0000
            ),
            TestCase(
                ZonedDateTime.parse("1970-01-01T00:00:00.000000000", formatter).toInstant(),
                ByteBuffer.allocate(Int.SIZE_BYTES).putInt(0).array() // 0x0000_0000
            ),
            TestCase(
                ZonedDateTime.parse("1970-01-01T00:00:01", formatter).toInstant(),
                ByteBuffer.allocate(Int.SIZE_BYTES).putInt(1).array() // 0x0000_0001
            ),
            TestCase(
                ZonedDateTime.parse("2106-02-07T06:28:15", formatter).toInstant(),
                ByteBuffer.allocate(Int.SIZE_BYTES).putInt(-1).array() // 0xFFFF_FFFF
            ),
            // timestamp 64 patterns
            TestCase(
                ZonedDateTime.parse("1970-01-01T00:00:00.000000001", formatter).toInstant(),
                run {
                    val epochSec = 0L // 1970-01-01 00:00:00 UTC
                    val nanoSec = 1L
                    // 0x0000000400000000
                    ByteBuffer.allocate(Long.SIZE_BYTES).putLong(nanoSec shl 34 or epochSec).array()
                }
            ),
            TestCase(
                // case for 0 nanosec and epoch sec is just 34bit
                Instant.ofEpochSecond(0x3_FFFF_FFFFL),
                run {
                    val epochSec = 0x3_FFFF_FFFFL
                    val nanoSec = 0L
                    // 0x0000000400000000
                    ByteBuffer.allocate(Long.SIZE_BYTES).putLong(nanoSec shl 34 or epochSec).array()
                }
            ),
            TestCase(
                ZonedDateTime.parse("2514-05-30T01:53:03.999999999", formatter).toInstant(),
                run {
                    val epochSec = 0x3_ffff_ffffL // 2514-05-30 01:53:03 UTC
                    val nanoSec = 999_999_999L
                    // 0xEE6B_27FF_FFFF_FFFF
                    ByteBuffer.allocate(Long.SIZE_BYTES).putLong(nanoSec shl 34 or epochSec).array()
                }
            ),
            // timestamp 96 patterns
            TestCase(
                ZonedDateTime.parse("2514-05-30T01:53:04", formatter).toInstant(),
                run {
                    val epochSec = 0x4_0000_0000L // 2514-05-30 01:53:04 UTC
                    val nanoSec = 0
                    // 0x0000_0000_0000_0004_ 0000_0000
                    ByteBuffer.allocate(Int.SIZE_BYTES + Long.SIZE_BYTES)
                        .putInt(nanoSec).putLong(epochSec).array()
                }
            ),
            TestCase(
                ZonedDateTime.parse("1969-12-31T23:59:59.999999999", formatter).toInstant(),
                run {
                    val epochSec = -1L // 1969-12-31T23:59:59 UTC
                    val nanoSec = 999_999_999
                    // 0x3B9A_C9FF_FFFF_FFFF_FFFF_FFFF
                    ByteBuffer.allocate(Int.SIZE_BYTES + Long.SIZE_BYTES)
                        .putInt(nanoSec).putLong(epochSec).array()
                }
            )
        )

        for (testCase in testCases) {
            val input = testCase.input
            val expected = testCase.expected
            val actual = encodeTimestamp(input.epochSecond, input.nano)
            val message = """
                $input should be 0x${expected.joinToString("") { "%02X".format(it) }}
            """.trimIndent()
            assertWithMessage(message).that(actual).isEqualTo(expected)
        }
    }
}
