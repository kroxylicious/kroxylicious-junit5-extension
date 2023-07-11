/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.invm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class LoggingPrintStreamTest {

    @Mock
    private System.Logger logger;

    @Test
    void singleLog() {
        try (var lps = LoggingPrintStream.loggingPrintStream(logger, System.Logger.Level.INFO)) {
            lps.println("Hello, world");

            verify(logger, times(1)).log(System.Logger.Level.INFO, "{0}", "Hello, world");
        }
        verifyNoMoreInteractions(logger);
    }

    @Test

    void singleLogGeneratedByManyPrints() {
        try (var lps = LoggingPrintStream.loggingPrintStream(logger, System.Logger.Level.INFO)) {
            lps.print("Hello,");
            lps.println(" world");

            verify(logger, times(1)).log(System.Logger.Level.INFO, "{0}", "Hello, world");
        }
        verifyNoMoreInteractions(logger);
    }

    @Test
    void twoLogs() {
        try (var lps = LoggingPrintStream.loggingPrintStream(logger, System.Logger.Level.INFO)) {
            lps.println("Hello, world");
            lps.println("Goodbye, cruel world");
            verify(logger, times(1)).log(System.Logger.Level.INFO, "{0}", "Hello, world");
            verify(logger, times(1)).log(System.Logger.Level.INFO, "{0}", "Goodbye, cruel world");
        }
        verifyNoMoreInteractions(logger);
    }

    @Test
    void closeLogsUnfinishedPrint() {
        // Given
        var lps = LoggingPrintStream.loggingPrintStream(logger, System.Logger.Level.INFO)) {
        lps.print("There's no newline");
        
        //When
        lps.close();
        
        //Then
        verify(logger, times(1)).log(System.Logger.Level.INFO, "{0}", "There's no newline");
        verifyNoMoreInteractions(logger);
    }

}