import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileWriter
import java.io.File
import java.io.FileOutputStream

class ParquetWriter {

    fun write(vectorSchemaRoot: VectorSchemaRoot, file: File) {
        FileOutputStream(file).use {
            ArrowFileWriter(vectorSchemaRoot, null, it.channel).use { writer ->
                {
                    writer.start()
                    writer.writeBatch()
                    writer.end()
                }
            }
        }
    }

}