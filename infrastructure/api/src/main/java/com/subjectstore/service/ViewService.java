package com.subjectstore.service;

import org.springframework.stereotype.Service;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectStore;
import systems.intino.datamarts.subjectstore.model.Subject;
import systems.intino.datamarts.subjectstore.SubjectHistoryView;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ViewService {

    public Map<String, Object> generateView(SubjectStore store, Subject subject, String yaml, File outputFile) throws Exception {
        SubjectHistory history = store.historyOf(subject);

        SubjectHistoryView.of(history)
                .with(yaml)
                .export()
                .to(new FileOutputStream(outputFile));

        List<List<Object>> data = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
            String line;
            boolean firstLine = true;
            while ((line = br.readLine()) != null) {
                List<String> values = Arrays.asList(line.trim().split("\\s+"));
                if (firstLine) {
                    columnNames.addAll(values);
                    firstLine = false;
                } else {
                    List<Object> row = values.stream().map(s -> {
                        try {
                            return Double.parseDouble(s);
                        } catch (NumberFormatException e) {
                            return s;
                        }
                    }).collect(Collectors.toList());
                    data.add(row);
                }
            }
        }

        return Map.of("columns", columnNames, "data", data);
    }
}