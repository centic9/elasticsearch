package org.elasticsearch.join.aggregations;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.join.aggregations.JoinAggregationBuilders.parent;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

public class ParentIT extends AbstractParentChildIT {

    public void testSimpleParentAgg() throws Exception {
        final SearchRequestBuilder searchRequest = client().prepareSearch("test")
            .setSize(10000)
            .setQuery(matchQuery("randomized", true))
            .addAggregation(
                parent("to_article", "comment")
                    .subAggregation(
                        terms("category").field("category").size(10000)));
        SearchResponse searchResponse = searchRequest.get();
        assertSearchResponse(searchResponse);

        final long comments = categoryToControl.values().stream().mapToLong(
            value -> value.commentIds.size()).sum();

        Parent parentAgg = searchResponse.getAggregations().get("to_article");
        assertThat("Request: " + searchRequest + "\nResponse: " + searchResponse + "\n",
            parentAgg.getDocCount(), equalTo(comments));
        Terms categoryTerms = parentAgg.getAggregations().get("category");
        assertThat(categoryTerms.getBuckets().size(),
            equalTo(categoryToControl.keySet().size()));
        for (String category : categoryToControl.keySet()) {
            final Terms.Bucket categoryBucket = categoryTerms.getBucketByKey(category);
            assertThat(categoryBucket.getKeyAsString(), equalTo(category));
            assertThat(categoryBucket.getDocCount(),
                equalTo((long)categoryToControl.get(category).commentIds.size()));
        }
    }

    public void testParentAggs() throws Exception {
        final SearchRequestBuilder searchRequest = client().prepareSearch("test")
            .setSize(10000)
            .setQuery(matchQuery("randomized", true))
            .addAggregation(
                terms("commenter").field("commenter").size(10000).subAggregation(parent("to_article", "comment")
                    .subAggregation(
                        terms("category").field("category").size(10000).subAggregation(
                            topHits("top_category")
                        ))
                )
            );
        SearchResponse searchResponse = searchRequest.get();
        assertSearchResponse(searchResponse);

        final Set<String> commenters = categoryToControl.values().stream().flatMap(
            (Function<Control, Stream<String>>) control -> control.commenterToCommentId.keySet().stream()).collect(Collectors.toSet());
        final Map<String, Set<String>> commenterToComments = new HashMap<>();
        for (Control control : categoryToControl.values()) {
            for (Map.Entry<String, Set<String>> entry : control.commenterToCommentId.entrySet()) {
                final Set<String> comments = commenterToComments.computeIfAbsent(entry.getKey(), s -> new HashSet<>());
                comments.addAll(entry.getValue());
            }
        }

        Terms categoryTerms = searchResponse.getAggregations().get("commenter");
        assertThat("Request: " + searchRequest + "\nResponse: " + searchResponse + "\n",
            categoryTerms.getBuckets().size(), equalTo(commenters.size()));
        for (String commenter : commenters) {
            Terms.Bucket categoryBucket = categoryTerms.getBucketByKey(commenter);
            assertThat(categoryBucket.getKeyAsString(), equalTo(commenter));
            assertThat(categoryBucket.getDocCount(), equalTo((long) commenterToComments.get(commenter).size()));

            Parent childrenBucket = categoryBucket.getAggregations().get("to_article");
            assertThat(childrenBucket.getName(), equalTo("to_article"));
            /*assertThat(childrenBucket.getDocCount(), equalTo((long) entry1.getValue().commentIds.size()));
            assertThat(((InternalAggregation)childrenBucket).getProperty("_count"),
                equalTo((long) entry1.getValue().commentIds.size()));

            Terms commentersTerms = childrenBucket.getAggregations().get("category");
            assertThat(((InternalAggregation)childrenBucket).getProperty("category"), sameInstance(commentersTerms));
            assertThat(commentersTerms.getBuckets().size(), equalTo(entry1.getValue().commenterToCommentId.size()));
            for (Map.Entry<String, Set<String>> entry2 : entry1.getValue().commenterToCommentId.entrySet()) {
                Terms.Bucket commentBucket = commentersTerms.getBucketByKey(entry2.getKey());
                assertThat(commentBucket.getKeyAsString(), equalTo(entry2.getKey()));
                assertThat(commentBucket.getDocCount(), equalTo((long) entry2.getValue().size()));

                TopHits topHits = commentBucket.getAggregations().get("top_commenters");
                for (SearchHit searchHit : topHits.getHits().getHits()) {
                    assertThat(entry2.getValue().contains(searchHit.getId()), is(true));
                }
            }*/
        }
    }

//    public void testNonExistingParentType() throws Exception {
//        SearchResponse searchResponse = client().prepareSearch("test")
//            .addAggregation(
//                parent("non-existing", "xyz")
//            ).get();
//        assertSearchResponse(searchResponse);
//
//        Parent parent = searchResponse.getAggregations().get("non-existing");
//        assertThat(parent.getName(), equalTo("non-existing"));
//        assertThat(parent.getDocCount(), equalTo(0L));
//    }
}
