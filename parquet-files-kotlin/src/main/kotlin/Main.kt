import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.LargeVarBinaryVector
import org.apache.arrow.vector.TimeStampSecTZVector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException

val longText = "Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature, discovered the undoubtable source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of \"de Finibus Bonorum et Malorum\" (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum, \"Lorem ipsum dolor sit amet..\", comes from a line in section 1.10.32.\n" +
            "The standard chunk of Lorem Ipsum used since the 1500s is reproduced below for those interested. Sections 1.10.32 and 1.10.33 from \"de Finibus Bonorum et Malorum\" by Cicero are also reproduced in their exact original form, accompanied by English versions from the 1914 translation by H. Rackham."

fun main(args: Array<String>) {
    writeParquetFile()
    readParquet()
}

fun readParquet() {
    val file = File("/Users/zoltan.czirjak.ext/Desktop/projects/parquet-files-kotlin/parquet-files-kotlin/out.parquet")
    try {
        RootAllocator().use { rootAllocator ->
            FileInputStream(file).use { fileInputStream ->
                ArrowFileReader(fileInputStream.channel, rootAllocator).use { reader ->
                    println("Metadata: " + reader.metaData)
                    println("Record batches in file: " + reader.recordBlocks.size)
                    println("Schema: " + reader.vectorSchemaRoot.schema)
                    println("MetaData in Schema: " + reader.vectorSchemaRoot.schema.customMetadata)

                    for (arrowBlock in reader.recordBlocks) {
                        reader.loadRecordBatch(arrowBlock)
                        val vectorSchemaRootRecover = reader.vectorSchemaRoot
                        println(vectorSchemaRootRecover.getVector("name"))
                        println(vectorSchemaRootRecover.getVector("age"))

                        val contentVector = vectorSchemaRootRecover.getVector("content") as VarBinaryVector
                        println("content 0")
                        println(contentVector[0].size)
                        contentVector[0].forEach { print("$it ") }
                        println()

                        println("content 1")
                        println(contentVector[1].size)
                        println(String(contentVector[1] as ByteArray))
                    }
                }
            }
        }
    } catch (e: IOException) {
        e.printStackTrace()
    }
}

fun writeParquetFile() {

    val headers = mapOf("name" to "Zoltan", "age" to "31", "now" to "Instant.now()")

    RootAllocator().use { allocator ->
        val content = Field(
            "content",
            FieldType.notNullable(ArrowType.Binary()),
            null
        )
        val name = Field(
            "name",
            FieldType.notNullable(ArrowType.Utf8()),
            null
        )
        val age = Field(
            "age",
            FieldType.notNullable(ArrowType.Int(32, true)),
            null
        )
        val schema = Schema(listOf(name, age, content), headers)

        VectorSchemaRoot.create(schema, allocator).use { vectorSchemaRoot ->

            val nameVector = vectorSchemaRoot.getVector("name") as VarCharVector
            nameVector.allocateNew(2)
            nameVector[0] = "Peter".toByteArray()
            nameVector[1] = "Kristof".toByteArray()

            val ageVector = vectorSchemaRoot.getVector("age") as IntVector
            ageVector.allocateNew(2)
            ageVector[0] = 10
            ageVector[1] = 20

            val contentVector = vectorSchemaRoot.getVector("content") as VarBinaryVector
            contentVector.allocateNew(2)
            contentVector[0] = byteArrayOf(1,2,3,4,5,6)
            contentVector[1] = longText.toByteArray()
            /**
             * If this 0:
                    Schema<content: Binary not null>(metadata: {name=Zoltan, age=31, now=Instant.now()})
                    content
             * If this 1:
                    Schema<content: Binary not null>(metadata: {name=Zoltan, age=31, now=Instant.now()})
                    content
                    [B@1c7696c6
             * If this is 2:
                    Schema<content: Binary not null>(metadata: {name=Zoltan, age=31, now=Instant.now()})
                    content
                    [B@1c7696c6
                    null
             */
            vectorSchemaRoot.rowCount = 2

            val file = File("/Users/zoltan.czirjak.ext/Desktop/projects/parquet-files-kotlin/parquet-files-kotlin/out.parquet")
            try {
                FileOutputStream(file).use { fileOutputStream ->
                    ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.channel).use { writer ->
                        writer.start()
                        writer.writeBatch()
                        writer.end()
                        println("Record batches written: " + writer.recordBlocks.size + ". Number of rows written: " + vectorSchemaRoot.rowCount)
                    }
                }
            } catch (e: IOException) {
                e.printStackTrace()
            }
        }
    }
}