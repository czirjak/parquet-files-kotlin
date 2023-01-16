import com.fasterxml.jackson.databind.ObjectMapper
import java.io.File

fun main(args: Array<String>) {
    val inputFile = File(ClassLoader.getSystemResource("test.json")!!.toURI())
    val outputFile = File(ClassLoader.getSystemResource("out.json")!!.toURI())


    val data = ObjectMapper().readValue<MyClass>(inputFile)

    val schema = MessageTypeParser.parseMessageType(
        "message MyClass {" +
                "  required int32 id;" +
                "  required binary name (UTF8);" +
                "}"
    )

    val path = Path("path/to/file.parquet")
    val writer = ParquetWriter.builder(path, GroupWriteSupport())
        .withType(schema)
        .build()

    val group = data.toGroup(schema)
    writer.write(group)

    writer.close()
}