package com.almworks.internal.interview.issuecache;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IssueCacheImpl implements IssueCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(IssueCacheImpl.class);

  private final IssueChangeTracker tracker;
  private final IssueLoader loader;
  private final Set<String> fieldIds;
  private final Map<Long, Map<String, Object>> cache = new HashMap<>();

  private IssueChangeTracker.Listener trackerListener;

  public IssueCacheImpl(IssueChangeTracker tracker, IssueLoader loader, Set<String> fieldIds) {
    this.tracker = tracker;
    this.loader = loader;
    this.fieldIds = fieldIds;
  }

  @Override
  public void subscribe(Set<Long> neededIssueIds, Listener listener) {
    this.trackerListener = (Set<Long> changedIssueIds) -> {
      Set<Long> neededChangedIssueIds = neededIssueIds.stream()
          .filter(changedIssueIds::contains)
          .collect(Collectors.toSet());
      LOGGER.info("Make a request to get issue IDs: {} with field IDs: {}", neededChangedIssueIds, this.fieldIds);
      this.loader.load(neededChangedIssueIds, this.fieldIds).thenAccept((IssueLoader.Result result) ->
          handleResult(result, listener));
    };
    this.tracker.subscribe(this.trackerListener);
    this.trackerListener.onIssuesChanged(neededIssueIds);
    LOGGER.info("A subscription to the issue cache has occurred.");
  }

  private void handleResult(IssueLoader.Result result, Listener listener) {
    for (long issueId: result.getIssueIds()) {
      Map<String, Object> fieldValues = result.getValues(issueId);
      listener.onIssueChanged(issueId, fieldValues);
      this.cache.put(issueId, fieldValues);
    }
    LOGGER.info("Cache updated fields for the issue IDs: {}", result.getIssueIds());
  }

  @Override
  public void unsubscribe(Listener listener) {
    this.tracker.unsubscribe(this.trackerListener);
    LOGGER.info("The issue cache subscription has been canceled.");
  }

  @Override
  public Object getField(long issueId, String fieldId) {
    Map<String, Object> fieldValues = cache.get(issueId);
    return fieldValues == null ? null : fieldValues.get(fieldId);
  }

  @Override
  public Set<String> getFieldIds() {
    return this.fieldIds;
  }
}
