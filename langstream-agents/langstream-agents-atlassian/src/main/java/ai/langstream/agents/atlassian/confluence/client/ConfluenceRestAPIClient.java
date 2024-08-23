package ai.langstream.agents.atlassian.confluence.client;

import ai.langstream.api.util.ObjectMapperFactory;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfluenceRestAPIClient {
    private final String domain;
    private final String basicHeader;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public ConfluenceRestAPIClient(String username, String apiToken, String domain) {
        this.domain = domain;
        this.basicHeader =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + apiToken)
                                                .getBytes(StandardCharsets.UTF_8));
    }

    private record Links(String next) {}

    private record SpaceObject(long id, String key, String name) {}

    private record SpacesResponse(List<SpaceObject> results, Links _links) {}

    public List<ConfluenceSpace> findSpaceByNameOrKeyOrId(String nameOrKeyOrId)
            throws IOException, InterruptedException {

        List<ConfluenceSpace> result = new ArrayList<>();

        String uri = "https://%s/wiki/api/v2/spaces?limit=250".formatted(domain);
        while (true) {
            HttpResponse<String> response = executeGet(uri);
            SpacesResponse spaces =
                    ObjectMapperFactory.getDefaultMapper()
                            .readValue(response.body(), SpacesResponse.class);
            for (SpaceObject space : spaces.results) {
                if (space.name.equals(nameOrKeyOrId)
                        || space.key.equals(nameOrKeyOrId)
                        || (space.id + "").equals(nameOrKeyOrId)) {
                    result.add(new ConfluenceSpace(space.id(), space.name(), space.key()));
                }
            }
            if (spaces._links().next() == null) {
                break;
            } else {
                uri = "https://%s%s".formatted(domain, spaces._links().next());
            }
        }

        return result;
    }

    private HttpResponse<String> executeGet(String uri) throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(uri))
                        .header("Authorization", basicHeader)
                        .GET()
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new RuntimeException(
                    "GET failed to "
                            + uri
                            + " with status "
                            + response.statusCode()
                            + " and body "
                            + response.body());
        }
        return response;
    }

    private record PageVersion(int number) {}

    private record PageObject(String id, String title, PageVersion version, String parentId) {}

    private record PagesResponse(List<PageObject> results, Links _links) {}

    @SneakyThrows
    public void visitSpacePages(
            long spaceId, Set<String> rootParents, Consumer<ConfluencePage> consumer) {
        List<ConfluencePage> result = new ArrayList<>();
        String uri =
                "https://%s/wiki/api/v2/spaces/%d/pages?limit=250&sort=modified-date"
                        .formatted(domain, spaceId);
        Map<String, String> pageToParent = new HashMap<>();
        while (true) {
            HttpResponse<String> response = executeGet(uri);
            PagesResponse pages =
                    ObjectMapperFactory.getDefaultMapper()
                            .readValue(response.body(), PagesResponse.class);
            for (PageObject page : pages.results) {
                if (!rootParents.isEmpty()) {
                    if (page.parentId() == null) {
                        continue;
                    }
                    pageToParent.put(page.id(), page.parentId());
                    result.add(
                            new ConfluencePage(
                                    spaceId,
                                    page.id(),
                                    page.title(),
                                    page.version().number() + ""));
                } else {
                    consumer.accept(
                            new ConfluencePage(
                                    spaceId,
                                    page.id(),
                                    page.title(),
                                    page.version().number() + ""));
                }
            }
            if (pages._links().next() == null) {
                break;
            } else {
                uri = "https://%s%s".formatted(domain, pages._links().next());
            }
        }
        if (rootParents.isEmpty()) {
            return;
        }
        for (ConfluencePage confluencePage : result) {
            String currentPageId = confluencePage.id();
            boolean include = false;
            while (true) {
                if (rootParents.contains(currentPageId)) {
                    include = true;
                    break;
                }
                String parentId = pageToParent.get(currentPageId);
                if (parentId == null) {
                    break;
                }
                currentPageId = parentId;
            }
            if (include) {
                consumer.accept(confluencePage);
            } else {
                log.debug("Skipping page {}", confluencePage);
            }
        }
    }

    private record ExportView(String value) {}

    private record ExportViewBody(ExportView export_view) {}

    private record PageObjectWithBody(ExportViewBody body) {}

    @SneakyThrows
    public byte[] exportPage(String pageId) {
        String pageUri =
                "https://%s/wiki/api/v2/pages/%s?body-format=export_view".formatted(domain, pageId);
        HttpResponse<String> pageBody = executeGet(pageUri);
        PageObjectWithBody pageObjectWithBody =
                ObjectMapperFactory.getDefaultMapper()
                        .readValue(pageBody.body(), PageObjectWithBody.class);
        String value = pageObjectWithBody.body().export_view().value();
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public void deletePageByTitle(long spaceId, String title) {
        visitSpacePages(
                spaceId,
                Set.of(),
                page -> {
                    if (page.title().equals(title)) {
                        deletePage(page.id());
                    }
                });
    }

    @SneakyThrows
    public void deletePage(String pageId) {
        String uri = "https://%s/wiki/api/v2/pages/%s".formatted(domain, pageId);
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(uri))
                        .header("Authorization", basicHeader)
                        .DELETE()
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new RuntimeException(
                    "DELETE failed to "
                            + uri
                            + " with status "
                            + response.statusCode()
                            + " and body "
                            + response.body());
        }
    }

    @SneakyThrows
    public String createPage(long spaceId, String title, String content, String parentId) {
        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put("spaceId", spaceId + "");
        payloadMap.put("status", "current");
        payloadMap.put("title", title);
        payloadMap.put(
                "body", Map.of("storage", Map.of("value", content, "representation", "storage")));
        if (parentId != null) {
            payloadMap.put("parentId", parentId);
        }
        String payload = ObjectMapperFactory.getDefaultMapper().writeValueAsString(payloadMap);

        String uri = "https://%s/wiki/api/v2/pages".formatted(domain);
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(uri))
                        .header("Authorization", basicHeader)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new RuntimeException(
                    "POST failed to "
                            + uri
                            + " with status "
                            + response.statusCode()
                            + " and body "
                            + response.body());
        }
        String body = response.body();
        return ObjectMapperFactory.getDefaultMapper().readValue(body, PageObject.class).id();
    }
}
