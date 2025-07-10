package com.subjectstore.controller;

import com.subjectstore.model.FeedRequest;
import com.subjectstore.model.ViewRequest;
import com.subjectstore.service.SubjectStoreService;
import com.subjectstore.service.ViewService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import systems.intino.datamarts.subjectstore.SubjectHistory;
import systems.intino.datamarts.subjectstore.SubjectStore;
import systems.intino.datamarts.subjectstore.model.Subject;

import java.io.File;
import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/subject")
public class SubjectController {

    private final SubjectStoreService storeService;
    private final ViewService viewService;

    public SubjectController(SubjectStoreService storeService, ViewService viewService) {
        this.storeService = storeService;
        this.viewService = viewService;
    }

    @PostMapping("/feed")
    public ResponseEntity<?> feed(@RequestBody FeedRequest req) {
        try {
            SubjectStore store = storeService.getOrCreateStore(req.id, req.type);
            Subject subject = store.create(req.id, req.type);
            req.attributes.forEach((k, v) -> subject.update().put(k, v.toString()));
            SubjectHistory history = store.historyOf(subject);
            SubjectHistory.Batch batch = history.batch();
            SubjectHistory.Transaction tx = history.on(Instant.parse(req.timestamp), req.source);
            req.attributes.forEach((k, v) -> {
                if (v instanceof Number) {
                    tx.put(k, ((Number) v).doubleValue());
                } else {
                    tx.put(k, v.toString());
                }
            });
            tx.terminate();
            batch.terminate();
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }


    @PostMapping("/view")
    public ResponseEntity<?> view(@RequestBody ViewRequest req) {
        try {
            SubjectStore store = storeService.getOrCreateStore(req.id, req.type);
            Subject subject = store.open(req.id, req.type);
            if (subject == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "Subject not found"));
            }
            File output = File.createTempFile("view-", ".tsv");
            Map<String, Object> result = viewService.generateView(store, subject, req.yaml, output);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}