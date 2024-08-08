package ai.langstream.agents.google.drive;

import ai.langstream.agents.google.cloudstorage.GoogleCloudStorageSource;
import ai.langstream.ai.agents.commons.storage.provider.StorageProviderObjectReference;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import com.google.api.services.drive.model.File;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class GoogleDriveSourceTest {


    @Test
    void test() throws Exception {
        GoogleDriveSource source = new GoogleDriveSource();
        source.initializeConfig(Map.of());
        File root = folder("root", null);
        File inside = folder("inside-folder", root.getId());

        List<File> files = List.of(
                root,
                inside,
                file("doc1", "application/vnd.google-apps.document", root.getId()),
                file("pdf1", "application/pdf", root.getId()),
                file("txt", "plain/text", root.getId()),
                file("out_txt", "plain/text", null),
                file("root_txt", "plain/text", null)
        );

        Map<String, List<String>> tree = new HashMap<>();
        Map<String, StorageProviderObjectReference> collect = new LinkedHashMap<>();
        source.inspectFiles(files, collect, tree);
        assertEquals(5, collect.size());
        assertEquals("doc1", collect.get("iddoc1").name());
        assertEquals("ckdoc1", collect.get("iddoc1").contentDigest());
        assertEquals(90L, collect.get("iddoc1").size());
    }


    @Test
    void testExcludeMime() throws Exception {
        GoogleDriveSource source = new GoogleDriveSource();
        source.initializeConfig(Map.of(
                "exclude-mime-types", "application/vnd.google-apps.document"
        ));
        File root = folder("root", null);
        File inside = folder("inside-folder", root.getId());

        List<File> files = List.of(
                root,
                inside,
                file("doc1", "application/vnd.google-apps.document", root.getId()),
                file("pdf1", "application/pdf", root.getId()),
                file("txt", "plain/text", root.getId()),
                file("out_txt", "plain/text", null),
                file("root_txt", "plain/text", null)
        );

        Map<String, List<String>> tree = new HashMap<>();
        Map<String, StorageProviderObjectReference> collect = new LinkedHashMap<>();
        source.inspectFiles(files, collect, tree);
        assertEquals(4, collect.size());
    }

    @Test
    void testIncludeOnly() throws Exception {
        GoogleDriveSource source = new GoogleDriveSource();
        source.initializeConfig(Map.of(
                "include-mime-types", "application/pdf,plain/text"
        ));
        File root = folder("root", null);
        File inside = folder("inside-folder", root.getId());

        List<File> files = List.of(
                root,
                inside,
                file("doc1", "application/vnd.google-apps.document", root.getId()),
                file("pdf1", "application/pdf", root.getId()),
                file("txt", "plain/text", root.getId()),
                file("out_txt", "plain/text", null),
                file("root_txt", "plain/text", null)
        );

        Map<String, List<String>> tree = new HashMap<>();
        Map<String, StorageProviderObjectReference> collect = new LinkedHashMap<>();
        source.inspectFiles(files, collect, tree);
        assertEquals(4, collect.size());
    }


    @Test
    void testRootFolders() throws Exception {
        GoogleDriveSource source = new GoogleDriveSource();
        File root = folder("root", null);
        File inside = folder("inside-folder", root.getId());
        source.initializeConfig(Map.of(
                "include-mime-types", "application/pdf,plain/text",
                "root-parents", inside.getId()
        ));

        List<File> files = List.of(
                root,
                inside,
                file("doc1", "application/vnd.google-apps.document", root.getId()),
                file("pdf1", "application/pdf", root.getId()),
                file("txt", "plain/text", root.getId()),
                file("out_txt", "plain/text", inside.getId()),
                file("root_txt", "plain/text", null)
        );

        Map<String, List<String>> tree = new HashMap<>();
        Map<String, StorageProviderObjectReference> collect = new LinkedHashMap<>();
        source.inspectFiles(files, collect, tree);
        source.filterRootParents(collect, tree);
        assertEquals(3, collect.size());
    }


    private static File file(String name, String mimeType, String parent) {
        File file = new File()
                .setName(name)
                .setId("id" + name)
                .setMimeType(mimeType)
                .setSize(90L)
                .setSha256Checksum("ck" + name);
        if (parent != null) {
            file.setParents(List.of(parent));
        }
        return file;
    }

    private static File folder(String name, String parent) {
        File file = new File()
                .setName(name)
                .setId("id" + name)
                .setMimeType("application/vnd.google-apps.folder")
                .setVersion(122L);
        if (parent != null) {
            file.setParents(List.of(parent));
        }
        return file;
    }


}