/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kafka.invm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LoggingPrintStreamTest {

    private static final System.Logger.Level LEVEL = System.Logger.Level.INFO;
    @Mock
    private System.Logger logger;

    @BeforeEach
    public void beforeEach() {
        when(logger.isLoggable(LEVEL)).thenReturn(true);
    }

    @AfterEach
    public void afterEach() {
        verifyNoMoreInteractions(logger);
    }

    @Test
    void singleLog() {
        try (var lps = LoggingPrintStream.loggingPrintStream(logger, LEVEL)) {
            lps.println("Hello, world");

            verify(logger).log(LEVEL, "{0}", "Hello, world");
        }
    }

    @Test
    void singleLogGeneratedByManyPrints() {
        try (var lps = LoggingPrintStream.loggingPrintStream(logger, LEVEL)) {
            lps.print("Hello,");
            lps.println(" world");

            verify(logger).log(LEVEL, "{0}", "Hello, world");
        }
    }

    @Test
    void twoLogs() {
        try (var lps = LoggingPrintStream.loggingPrintStream(logger, LEVEL)) {
            lps.println("Hello, world");
            lps.println("Goodbye, cruel world");
            verify(logger).log(LEVEL, "{0}", "Hello, world");
            verify(logger).log(LEVEL, "{0}", "Goodbye, cruel world");
        }
    }

    @Test
    void logsDuplicates() {
        try (var lps = LoggingPrintStream.loggingPrintStream(logger, LEVEL)) {
            lps.println("Hello, world");
            lps.println("Hello, world");
            verify(logger, times(2)).log(LEVEL, "{0}", "Hello, world");
        }
    }

    @Test
    void respectsDynamicLevelChanges() {
        when(logger.isLoggable(LEVEL)).thenReturn(false);

        try (var lps = LoggingPrintStream.loggingPrintStream(logger, LEVEL)) {
            lps.println("I won't be logged");
            when(logger.isLoggable(LEVEL)).thenReturn(true);
            lps.println("But I will");

            verify(logger).log(LEVEL, "{0}", "But I will");
        }
    }

    @Test
    void closeLogsUnfinishedPrint() {
        // Given
        var lps = LoggingPrintStream.loggingPrintStream(logger, LEVEL);
        lps.print("There's no newline");

        // When
        lps.close();

        // Then
        verify(logger).log(LEVEL, "{0}", "There's no newline");
    }

}