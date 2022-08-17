package org.astraea.app.performance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class ReportFormatTest {
    @Test
    void testInitCSV() throws IOException{
        var stringWriter = new StringWriter();
        var writer = new BufferedWriter(stringWriter);
        var elements = List.of(titleValue("t1", "1"), titleValue("t2", "2"));

        ReportFormat.initCSVFormat(writer, elements);
        writer.flush();

        Assertions.assertEquals("t1, t2, \n", stringWriter.toString());
    }
    @Test
    void testLogToCSV() throws IOException, InterruptedException {
        StringWriter stringWriter = new StringWriter();
        var writer = new BufferedWriter(stringWriter);
        var elements = List.of(titleValue("t1", "1"), titleValue("t2", "2"));

        ReportFormat.logToCSV(writer, elements);
        writer.flush();

        Assertions.assertEquals("1, 2, \n", stringWriter.toString());
    }

    private static ReportFormat.CSVContentElement titleValue(String title, String value){
        return ReportFormat.CSVContentElement.create(title, ()-> value);
    }
}
