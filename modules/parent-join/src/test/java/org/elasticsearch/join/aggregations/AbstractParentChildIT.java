package org.elasticsearch.join.aggregations;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.join.query.ParentChildTestCase;
import org.junit.Before;

public abstract class AbstractParentChildIT  extends ParentChildTestCase {
    protected static final Map<String, Control> categoryToControl = new HashMap<>();


    @Before
    public void setupCluster() throws Exception {
        categoryToControl.clear();
        if (legacy()) {
            assertAcked(
                prepareCreate("test")
                    .addMapping("article", "category", "type=keyword")
                    .addMapping("comment", "_parent", "type=article", "commenter", "type=keyword")
            );
        } else {
            assertAcked(
                prepareCreate("test")
                    .addMapping("doc",
                        addFieldMappings(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "article", "comment"),
                            "commenter", "keyword", "category", "keyword"))
            );
        }

        List<IndexRequestBuilder> requests = new ArrayList<>();
        String[] uniqueCategories = new String[randomIntBetween(1, 25)];
        for (int i = 0; i < uniqueCategories.length; i++) {
            uniqueCategories[i] = Integer.toString(i);
        }
        int catIndex = 0;

        int numParentDocs = randomIntBetween(uniqueCategories.length, uniqueCategories.length * 5);
        for (int i = 0; i < numParentDocs; i++) {
            String id = "article-" + i;

            // TODO: this array is always of length 1, and testChildrenAggs fails if this is changed
            String[] categories = new String[randomIntBetween(1,1)];
            for (int j = 0; j < categories.length; j++) {
                String category = categories[j] = uniqueCategories[catIndex++ % uniqueCategories.length];
                ChildrenIT.Control control = categoryToControl.get(category);
                if (control == null) {
                    categoryToControl.put(category, control = new ChildrenIT.Control(category));
                }
                control.articleIds.add(id);
            }

            requests.add(createIndexRequest("test", "article", id, null, "category", categories, "randomized", true));
        }

        String[] commenters = new String[randomIntBetween(5, 50)];
        for (int i = 0; i < commenters.length; i++) {
            commenters[i] = Integer.toString(i);
        }

        int id = 0;
        for (ChildrenIT.Control control : categoryToControl.values()) {
            for (String articleId : control.articleIds) {
                int numChildDocsPerParent = randomIntBetween(0, 5);
                for (int i = 0; i < numChildDocsPerParent; i++) {
                    String commenter = commenters[id % commenters.length];
                    String idValue = "comment-" + id++;
                    control.commentIds.add(idValue);
                    Set<String> ids = control.commenterToCommentId.get(commenter);
                    if (ids == null) {
                        control.commenterToCommentId.put(commenter, ids = new HashSet<>());
                    }
                    ids.add(idValue);
                    requests.add(createIndexRequest("test", "comment", idValue, articleId, "commenter", commenter));
                }
            }
        }

        requests.add(createIndexRequest("test", "article", "a", null, "category", new String[]{"a"}, "randomized", false));
        requests.add(createIndexRequest("test", "article", "b", null, "category", new String[]{"a", "b"}, "randomized", false));
        requests.add(createIndexRequest("test", "article", "c", null, "category", new String[]{"a", "b", "c"}, "randomized", false));
        requests.add(createIndexRequest("test", "article", "d", null, "category", new String[]{"c"}, "randomized", false));
        requests.add(createIndexRequest("test", "comment", "e", "a"));
        requests.add(createIndexRequest("test", "comment", "f", "c"));

        indexRandom(true, requests);
        ensureSearchable("test");
    }


    protected static final class Control {

        final String category;
        final Set<String> articleIds = new HashSet<>();
        final Set<String> commentIds = new HashSet<>();
        final Map<String, Set<String>> commenterToCommentId = new HashMap<>();

        private Control(String category) {
            this.category = category;
        }
    }
}
