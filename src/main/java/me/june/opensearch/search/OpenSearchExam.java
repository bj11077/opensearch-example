package me.june.opensearch.search;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.opensearch.client.opensearch.core.*;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


@Slf4j
@Component
@RequiredArgsConstructor
public class OpenSearchExam {


    private final OpenSearchClient client;

    @Value("${opensearch.index}")
    private String index;

    /**
     *  Search by timeField & Pagination
     */
    public void findByTimeField() throws IOException {
        SearchRequest request = new SearchRequest.Builder().index(index)
                .query(builder -> builder.range(r-> r.field("systemTime").gte(JsonData.of(ZonedDateTime.now().minusDays(2).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) ))))
                .from(0).size(100).build();
        SearchResponse<saveDto> response = client.search(request, saveDto.class);
        System.out.println(response.hits().hits().size());
        response.hits().hits().forEach(e->{
            System.out.println(e.source().systemTime);
        });
    }

    /**
     *  insert Data
     */
    public void insertData(){

        IndexRequest<Object> indexRequest = IndexRequest.of(builder -> builder
                .index(index)
                .document(new saveDto("15","ss", ZonedDateTime.now().toInstant().toEpochMilli()))
        );
        try {
            IndexResponse response = client.index(indexRequest);
            System.out.println(response.result().name());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // delete Doc
    public void deleteDoc() throws IOException {
        client.delete(b -> b.index("test-index").id("1"));
    }


    public record saveDto(String id,String name, long systemTime){
    }
}
