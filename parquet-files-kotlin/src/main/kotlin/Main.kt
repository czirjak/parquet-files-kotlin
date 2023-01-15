import org.apache.arrow.vector.VectorSchemaRoot
import java.io.File

fun main(args: Array<String>) {
    val inputFile = File(ClassLoader.getSystemResource("test.json")!!.toURI())


    val parquetReader = ParquetReader()


    parquetReader.read(inputFile)

    val vectorSchemaRoot = VectorSchemaRoot.

    val outputFile = File(ClassLoader.getSystemResource("out.json")!!.toURI())
    val parquetWriter = ParquetWriter()
    parquetWriter.write(vectorSchemaRoot, outputFile)
}