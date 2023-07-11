/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.invm;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;

final class LoggingPrintStream {

    static PrintStream loggingPrintStream(System.Logger logger, System.Logger.Level level) {
        return new PrintStream(new LoggingOutputStream(logger, level));
    }

    private LoggingPrintStream() {
    }

    private static class LoggingOutputStream extends OutputStream {
        private final ByteArrayOutputStream os = new ByteArrayOutputStream(1000);
        private final System.Logger logger;
        private final System.Logger.Level level;

        public LoggingOutputStream(System.Logger logger, System.Logger.Level level) {
            this.logger = logger;
            this.level = level;
        }

        @Override
        public void write(int b) {
            if (b == '\n' || b == '\r') {
                logPending();
            }
            else {
                os.write(b);
            }
        }

        @Override
        public void close() {
            logPending();
        }

        private void logPending() {
            if (logger.isLoggable(level)) {
                var log = os.toString(Charset.defaultCharset()).stripTrailing();
                if (!log.isEmpty()) {
                    logger.log(level, "{0}", log);
                }
            }
            os.reset();
        }
    }

}
