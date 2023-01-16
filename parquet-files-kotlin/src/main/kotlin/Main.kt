import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.LargeVarBinaryVector
import org.apache.arrow.vector.TimeStampSecTZVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.time.Instant


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
                    for (arrowBlock in reader.recordBlocks) {
                        reader.loadRecordBatch(arrowBlock)
                        val vectorSchemaRootRecover = reader.vectorSchemaRoot
                        println(vectorSchemaRootRecover.schema)
                        print(vectorSchemaRootRecover.contentToTSVString())
                    }
                }
            }
        }
    } catch (e: IOException) {
        e.printStackTrace()
    }
}

fun writeParquetFile() {

    val headers = mapOf("name" to "Zoltan", "age" to 31, "now" to Instant.now())

    RootAllocator().use { allocator ->
        val content = Field(
            "content",
            FieldType.nullable(ArrowType.Binary()),
            null
        )
        val schema = Schema(listOf(content))
        val metadata = FileMetadata()

        VectorSchemaRoot.create(schema, allocator).use { vectorSchemaRoot ->
            val nameVector = vectorSchemaRoot.getVector("name") as VarCharVector
            nameVector.allocateNew(3)
            nameVector[0] = "Peter".toByteArray()
            nameVector[1] = "Kristof".toByteArray()
            val ageVector = vectorSchemaRoot.getVector("age") as IntVector
            ageVector.allocateNew(3)
            ageVector[0] = 10
            ageVector[1] = 20
            val birthdayVector = vectorSchemaRoot.getVector("birthday") as TimeStampSecTZVector
            birthdayVector.allocateNew(3)
            birthdayVector[0] = 10
            birthdayVector[1] = 20
            val contentVector = vectorSchemaRoot.getVector("content") as LargeVarBinaryVector
            contentVector.allocateNew(3)
            contentVector[0] = 10
            contentVector[1] = 20
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