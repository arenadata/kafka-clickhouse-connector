/*
 * Copyright © 2021 Kafka Clickhouse Reader
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.kafka.clickhouse.reader.avro;

import io.arenadata.kafka.clickhouse.reader.avro.exception.NotImplementedCompressFunctionException;
import org.apache.avro.file.CodecFactory;

public class CompressorCodec {

  public static final CodecFactory ZSTD = CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL);
  public static final CodecFactory BZIP2 = CodecFactory.bzip2Codec();
  public static final CodecFactory DEFLATE = CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
  public static final CodecFactory SNAPPY = CodecFactory.snappyCodec();
  public static final CodecFactory XZ = CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
  public static final CodecFactory NONE = CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);

  public static CodecFactory byAlgorithm(String algorithm) {
    switch (algorithm) {
      case "zstd": return ZSTD;
      case "bzip2": return BZIP2;
      case "deflate": return DEFLATE;
      case "xz": return XZ;
      case "snappy": return SNAPPY;
      case "none": return NONE;
      default: throw new NotImplementedCompressFunctionException(null, algorithm);
    }
  }

}
