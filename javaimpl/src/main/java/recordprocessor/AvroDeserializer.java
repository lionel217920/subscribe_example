package recordprocessor;

import com.alibaba.dts.formats.avro.Record;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Avro反序列化
 */
public class AvroDeserializer {

    private static final Logger log = LoggerFactory.getLogger(AvroDeserializer.class);

    /**
     * Avro反序列化的API
     */
    private final SpecificDatumReader<Record> reader = new SpecificDatumReader<Record>(com.alibaba.dts.formats.avro.Record.class);

    public AvroDeserializer() {

    }

    /**
     * Avro反序列化，将kafka消息反序列化成Record
     *
     * @param data kafka消息二进制数据
     * @return Record
     */
    public com.alibaba.dts.formats.avro.Record deserialize(byte[] data) {

        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        Record payload = null;
        try {
            payload = reader.read(null, decoder);
            return payload;
        }catch (Throwable ex) {
            log.error("AvroDeserializer: deserialize record failed cause " + ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }
}
