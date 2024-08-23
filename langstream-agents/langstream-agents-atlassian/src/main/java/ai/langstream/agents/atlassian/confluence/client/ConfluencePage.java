package ai.langstream.agents.atlassian.confluence.client;

public record ConfluencePage(long spaceId, String id, String title, String pageVersion) {

}
