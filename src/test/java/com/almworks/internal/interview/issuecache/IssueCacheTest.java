package com.almworks.internal.interview.issuecache;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * You are expected to write unit tests for Issue Cache.
 * */
public class IssueCacheTest {
  @Before
  public void setupSubscriptionTo12() {
    assertThat(myCache.getField(1L, "key"), nullValue());

    myCache.subscribe(Set.of(1L, 2L), listener12);

    assertThat(myLoader.requests, hasSize(1));

    assertThat(myCache.getField(1L, "key"), nullValue());

    myLoader.reply(Map.of(1L, issue1, 2L, issue2));

    verify(listener12).onIssueChanged(1L, issue1);
    verify(listener12).onIssueChanged(2L, issue2);
    verifyNoMoreInteractions(listener12);
    reset(listener12);

    assertThat(myCache.getField(1L, "key"), is("k1"));
    assertThat(myCache.getField(2L, "summary"), is("s2"));
  }

  @Test
  public void basic() {
    // Checks that setupSubscriptionTo12() works

    assertThat(myCache.getField(1L, "key"), is("k1"));
    assertThat(myCache.getField(1L, "summary"), is("s1"));
    assertThat(myCache.getField(2L, "key"), is("k2"));
    assertThat(myCache.getField(2L, "summary"), is("s2"));

    assertThat(myCache.getField(3L, "key"), nullValue());
    assertThat(myCache.getField(3L, "summary"), nullValue());
    assertThat(myCache.getField(2L, "s"), nullValue());

    myTracker.recordChanges(Set.of(0L, 1L, 2L, 3L, 4L, 5L));
    assertThat(myLoader.requests, hasSize(1));
    checkRequests(List.of(1L, 2L));

    assertThat(myCache.getField(1L, "key"), is("k1"));
    assertThat(myCache.getField(1L, "summary"), is("s1"));
    assertThat(myCache.getField(2L, "key"), is("k2"));
    assertThat(myCache.getField(2L, "summary"), is("s2"));

    myLoader.reply(Map.of(1L, issue21, 2L, issue21));

    verify(listener12).onIssueChanged(1L, issue21);
    verify(listener12).onIssueChanged(2L, issue21);
    verifyNoMoreInteractions(listener12);

    assertThat(myCache.getField(1L, "key"), is("k21"));
    assertThat(myCache.getField(1L, "summary"), is("s21"));
    assertThat(myCache.getField(2L, "key"), is("k21"));
    assertThat(myCache.getField(2L, "summary"), is("s21"));
  }

  @Test
  public void changeTriggersUpdate() {
    myTracker.recordChanges(Set.of(2L));

    assertThat(myLoader.requests, hasSize(1));
    checkRequests(List.of(2L));
    assertThat(myCache.getField(2L, "key"), is("k2"));

    myLoader.reply(Map.of(2L, issue21));

    verify(listener12).onIssueChanged(2L, issue21);
    verifyNoMoreInteractions(listener12);

    assertThat(myCache.getField(2L, "key"), is("k21"));
  }

  @SafeVarargs
  private void checkRequests(List<Long>... requests) {
    assertThat(myLoader.requests, IsIterableContainingInOrder.contains(
      Arrays.stream(requests).map(r ->
        hasProperty("issueIds", containsInAnyOrder(r.toArray()))
      ).collect(Collectors.toList())
    ));
  }

  private final MockIssueChangeTracker myTracker = new MockIssueChangeTracker();
  private final MockIssueLoader myLoader = new MockIssueLoader();
  private final IssueCache myCache = new IssueCacheImpl(myTracker, myLoader, Set.of("key", "summary"));

  private final IssueCache.Listener listener12 = mock(IssueCache.Listener.class);

  private final Map<String, Object> issue1 = Map.of("key", "k1", "summary", "s1");
  private final Map<String, Object> issue2 = Map.of("key", "k2", "summary", "s2");
  private final Map<String, Object> issue21 = Map.of("key", "k21", "summary", "s21");


  private static class MockIssueChangeTracker implements IssueChangeTracker {
    private IssueChangeTracker.Listener myListener;

    @Override
    public void subscribe(Listener listener) {
      assertThat("unexpected second call to subscribe()", myListener, nullValue());
      myListener = listener;
    }

    public void recordChanges(Set<Long> changedIds) {
      myListener.onIssuesChanged(changedIds);
    }

    @Override
    public void unsubscribe(Listener listener) {
      fail("Unexpected call to unsubscribe()");
    }
  }


  private static class MockIssueLoader implements IssueLoader {
    public List<IssueLoadRequest> requests = new ArrayList<>();

    @Override
    public CompletionStage<LoadResult> load(Collection<Long> issueIds, Set<String> fieldIds) {
      IssueLoadRequest request = new IssueLoadRequest(issueIds, fieldIds);
      requests.add(request);
      request.resultFuture.handle((r, t) -> requests.remove(request));
      return request.resultFuture;
    }

    public void reply(Map<Long, Map<String, Object>> result) {
      assertThat(requests, not(empty()));
      requests.get(0).resolve(result);
    }
  }


  public static class IssueLoadRequest {
    public final List<Long> issueIds;
    public final Set<String> fieldIds;
    public final CompletableFuture<LoadResult> resultFuture = new CompletableFuture<>();

    public IssueLoadRequest(Collection<Long> issueIds, Set<String> fieldIds) {
      this.issueIds = issueIds.stream().sorted().collect(Collectors.toList());
      this.fieldIds = fieldIds;
    }

    public Collection<Long> getIssueIds() {
      return issueIds;
    }

    public Set<String> getFieldIds() {
      return fieldIds;
    }

    public CompletableFuture<LoadResult> getResultFuture() {
      return resultFuture;
    }

    public void resolve(Map<Long, Map<String, Object>> result) {
      assertThat(issueIds, containsInAnyOrder(result.keySet().toArray(Long[]::new)));
      resultFuture.complete(new LoadResult(this, result));
    }
  }


  private static class LoadResult implements IssueLoader.Result {
    private final IssueLoadRequest myRequest;
    private final Map<Long, Map<String, Object>> myResult;

    private LoadResult(IssueLoadRequest req, Map<Long, Map<String, Object>> result) {
      myRequest = req;
      myResult = result;
    }

    @Override
    public Map<String, Object> getValues(long issueId) {
      return myResult.get(issueId);
    }

    @Override
    public Set<Long> getIssueIds() {
      return myResult.keySet();
    }

    @Override
    public Set<String> getFieldIds() {
      return myRequest.fieldIds;
    }
  }
}
