import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.JsonFileReader
import java.io.File

class ParquetReader {

    fun read(file: File): VectorSchemaRoot {
        val allocator = RootAllocator(10 * 1024)
        val reader = JsonFileReader(file, allocator)

        reader.start()             //to parse schema
        return reader.read()
    }

}