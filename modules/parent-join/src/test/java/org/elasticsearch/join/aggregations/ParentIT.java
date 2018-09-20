package org.elasticsearch.join.aggregations;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.join.aggregations.JoinAggregationBuilders.parent;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

public class ParentIT extends AbstractParentChildIT {

    public void testParentAggs() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(matchQuery("randomized", true))
            .addAggregation(
                terms("commenter").field("commenter").size(10000).subAggregation(parent("to_article", "article")
                    .subAggregation(
                        terms("category").field("cateogry").size(10000).subAggregation(
                            topHits("top_commenters")
                        ))
                )
            ).get();
        assertSearchResponse(searchResponse);

        Terms categoryTerms = searchResponse.getAggregations().get("commenter");
        assertThat("Having: " + searchResponse,
            categoryTerms.getBuckets().size(), equalTo(categoryToControl.size()));
        for (Map.Entry<String, Control> entry1 : categoryToControl.entrySet()) {
            Terms.Bucket categoryBucket = categoryTerms.getBucketByKey(entry1.getKey());
            assertThat(categoryBucket.getKeyAsString(), equalTo(entry1.getKey()));
            assertThat(categoryBucket.getDocCount(), equalTo((long) entry1.getValue().articleIds.size()));

            Children childrenBucket = categoryBucket.getAggregations().get("to_article");
            assertThat(childrenBucket.getName(), equalTo("to_article"));
            assertThat(childrenBucket.getDocCount(), equalTo((long) entry1.getValue().commentIds.size()));
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
            }
        }
    }

    public void testNonExistingParentType() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
            .addAggregation(
                parent("non-existing", "xyz")
            ).get();
        assertSearchResponse(searchResponse);

        Parent parent = searchResponse.getAggregations().get("non-existing");
        assertThat(parent.getName(), equalTo("non-existing"));
        assertThat(parent.getDocCount(), equalTo(0L));
    }
}
