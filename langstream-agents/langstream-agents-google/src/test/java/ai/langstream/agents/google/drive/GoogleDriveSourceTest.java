/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.agents.google.drive;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.ai.agents.commons.storage.provider.StorageProviderObjectReference;
import com.google.api.services.drive.model.File;
import java.util.*;
import org.junit.jupiter.api.Test;

class GoogleDriveSourceTest {

    @Test
    void test() throws Exception {
        GoogleDriveSource source = new GoogleDriveSource();
        source.initializeConfig(Map.of());
        File root = folder("root", null);
        File inside = folder("inside-folder", root.getId());

        List<File> files =
                List.of(
                        root,
                        inside,
                        file("doc1", "application/vnd.google-apps.document", root.getId()),
                        file("pdf1", "application/pdf", root.getId()),
                        file("txt", "plain/text", root.getId()),
                        file("out_txt", "plain/text", null),
                        file("root_txt", "plain/text", null));

        Map<String, List<String>> tree = new HashMap<>();
        Map<String, StorageProviderObjectReference> collect = new LinkedHashMap<>();
        source.inspectFiles(files, collect, tree);
        assertEquals(5, collect.size());
        assertEquals("iddoc1", collect.get("iddoc1").name());
        assertEquals("ckdoc1", collect.get("iddoc1").contentDigest());
        assertEquals(90L, collect.get("iddoc1").size());
    }

    @Test
    void testExcludeMime() throws Exception {
        GoogleDriveSource source = new GoogleDriveSource();
        source.initializeConfig(
                Map.of("exclude-mime-types", "application/vnd.google-apps.document"));
        File root = folder("root", null);
        File inside = folder("inside-folder", root.getId());

        List<File> files =
                List.of(
                        root,
                        inside,
                        file("doc1", "application/vnd.google-apps.document", root.getId()),
                        file("pdf1", "application/pdf", root.getId()),
                        file("txt", "plain/text", root.getId()),
                        file("out_txt", "plain/text", null),
                        file("root_txt", "plain/text", null));

        Map<String, List<String>> tree = new HashMap<>();
        Map<String, StorageProviderObjectReference> collect = new LinkedHashMap<>();
        source.inspectFiles(files, collect, tree);
        assertEquals(4, collect.size());
    }

    @Test
    void testIncludeOnly() throws Exception {
        GoogleDriveSource source = new GoogleDriveSource();
        source.initializeConfig(Map.of("include-mime-types", "application/pdf,plain/text"));
        File root = folder("root", null);
        File inside = folder("inside-folder", root.getId());

        List<File> files =
                List.of(
                        root,
                        inside,
                        file("doc1", "application/vnd.google-apps.document", root.getId()),
                        file("pdf1", "application/pdf", root.getId()),
                        file("txt", "plain/text", root.getId()),
                        file("out_txt", "plain/text", null),
                        file("root_txt", "plain/text", null));

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
        source.initializeConfig(
                Map.of(
                        "include-mime-types",
                        "application/pdf,plain/text",
                        "root-parents",
                        root.getId()));

        List<File> files =
                List.of(
                        root,
                        inside,
                        file("doc1", "application/vnd.google-apps.document", root.getId()),
                        file("pdf1", "application/pdf", root.getId()),
                        file("txt", "plain/text", root.getId()),
                        file("out_txt", "plain/text", inside.getId()),
                        file("root_txt", "plain/text", null));

        Map<String, List<String>> tree = new HashMap<>();
        Map<String, StorageProviderObjectReference> collect = new LinkedHashMap<>();
        source.inspectFiles(files, collect, tree);
        source.filterRootParents(collect, tree);
        assertEquals(3, collect.size());
    }

    private static File file(String name, String mimeType, String parent) {
        File file =
                new File()
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
        File file =
                new File()
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
