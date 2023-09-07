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
package ai.langstream.runtime.agent.nar;

import ai.langstream.api.codestorage.GenericZipFileArchiveFile;
import ai.langstream.api.codestorage.LocalZipFileArchiveFile;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NarFileHandler
        implements AutoCloseable,
                AgentCodeRegistry.AgentPackageLoader,
                AssetManagerRegistry.AssetManagerPackageLoader {

    private final Path packagesDirectory;
    private final Path temporaryDirectory;

    private final ClassLoader customCodeClassloader;

    private List<URLClassLoader> classloaders;
    private final Map<String, PackageMetadata> packages = new HashMap<>();

    public NarFileHandler(Path packagesDirectory, ClassLoader customCodeClassloader)
            throws Exception {
        this.packagesDirectory = packagesDirectory;
        this.temporaryDirectory = Files.createTempDirectory("nar");
        this.customCodeClassloader = customCodeClassloader;
    }

    private static void deleteDirectory(Path dir) throws Exception {
        try (DirectoryStream<Path> all = Files.newDirectoryStream(dir)) {
            for (Path file : all) {
                if (Files.isDirectory(file)) {
                    deleteDirectory(file);
                } else {
                    Files.delete(file);
                }
            }
            Files.delete(dir);
        }
    }

    public void close() {

        for (PackageMetadata metadata : packages.values()) {
            URLClassLoader classLoader = metadata.getClassLoader();
            if (classLoader != null) {
                try {
                    classLoader.close();
                } catch (Exception err) {
                    log.error("Cannot close classloader {}", classLoader, err);
                }
            }
        }

        try {
            deleteDirectory(temporaryDirectory);
        } catch (Exception e) {
            log.error("Cannot delete temporary directory {}", temporaryDirectory, e);
        }
    }

    @Data
    final class PackageMetadata {
        private final Path nar;
        private final String name;
        private final Set<String> agentTypes;
        private final Set<String> assetTypes;
        private Path directory;
        private URLClassLoader classLoader;

        public PackageMetadata(
                Path nar, String name, Set<String> agentTypes, Set<String> assetTypes) {
            this.nar = nar;
            this.name = name;
            this.agentTypes = agentTypes;
            this.assetTypes = assetTypes;
        }

        public void unpack() throws Exception {
            if (directory != null) {
                return;
            }
            Path dest = temporaryDirectory.resolve(nar.getFileName().toString() + ".dir");
            log.info("Unpacking NAR file {} to {}", nar, dest);
            GenericZipFileArchiveFile file = new LocalZipFileArchiveFile(nar);
            file.extractTo(dest);
            directory = dest;
        }
    }

    public void scan() throws Exception {
        try (DirectoryStream<Path> all = Files.newDirectoryStream(packagesDirectory, "*.nar")) {
            for (Path narFile : all) {
                handleNarFile(narFile);
            }
        } catch (Exception err) {
            log.error("Failed to scan packages directory", err);
            throw err;
        }
    }

    public void handleNarFile(Path narFile) throws Exception {
        String filename = narFile.getFileName().toString();

        // first of all we look for an index file
        try (ZipFile zipFile = new ZipFile(narFile.toFile())) {

            List<String> agents = List.of();
            List<String> assetTypes = List.of();

            ZipEntry entryAgentsIndex = zipFile.getEntry("META-INF/ai.langstream.agents.index");
            if (entryAgentsIndex != null) {
                InputStream inputStream = zipFile.getInputStream(entryAgentsIndex);
                byte[] bytes = inputStream.readAllBytes();
                String string = new String(bytes, StandardCharsets.UTF_8);
                BufferedReader reader = new BufferedReader(new StringReader(string));
                agents = reader.lines().filter(s -> !s.isBlank() && !s.startsWith("#")).toList();
                log.info(
                        "The file {} contains a static agents index, skipping the unpacking. It is expected that handles these agents: {}",
                        narFile,
                        agents);
            }

            ZipEntry entryAssetsIndex = zipFile.getEntry("META-INF/ai.langstream.assets.index");
            if (entryAssetsIndex != null) {
                InputStream inputStream = zipFile.getInputStream(entryAssetsIndex);
                byte[] bytes = inputStream.readAllBytes();
                String string = new String(bytes, StandardCharsets.UTF_8);
                BufferedReader reader = new BufferedReader(new StringReader(string));
                assetTypes =
                        reader.lines().filter(s -> !s.isBlank() && !s.startsWith("#")).toList();
                log.info(
                        "The file {} contains a static assetTypes index, skipping the unpacking. It is expected that handles these assetTypes: {}",
                        narFile,
                        assetTypes);
            }

            if (!agents.isEmpty() || !assetTypes.isEmpty()) {
                PackageMetadata metadata =
                        new PackageMetadata(
                                narFile, filename, Set.copyOf(agents), Set.copyOf(assetTypes));
                packages.put(filename, metadata);
                return;
            }

            ZipEntry serviceProviderForAgents =
                    zipFile.getEntry(
                            "META-INF/services/ai.langstream.api.runner.code.AgentCodeProvider");
            ZipEntry serviceProviderForAssets =
                    zipFile.getEntry(
                            "META-INF/services/ai.langstream.api.runner.assets.AssetManagerProvider");
            if (serviceProviderForAgents == null && serviceProviderForAssets == null) {
                log.info(
                        "The file {} does not contain any AgentCodeProvider/AssetManagerProvider, skipping the file",
                        narFile);
                return;
            }
        }

        log.info("The file {} does not contain any index, still adding the file", narFile);
        PackageMetadata metadata = new PackageMetadata(narFile, filename, null, null);
        packages.put(filename, metadata);
    }

    @Override
    public AssetManagerRegistry.AssetPackage loadPackageForAsset(String assetType)
            throws Exception {
        PackageMetadata packageForAssetType = getPackageForAssetType(assetType);
        if (packageForAssetType == null) {
            return null;
        }
        URLClassLoader classLoader =
                createClassloaderForPackage(customCodeClassloader, packageForAssetType);
        log.info(
                "For package {}, classloader {}, parent {}",
                packageForAssetType.getName(),
                classLoader,
                classLoader.getParent());
        return new AssetManagerRegistry.AssetPackage() {
            @Override
            public ClassLoader getClassloader() {
                return classLoader;
            }

            @Override
            public String getName() {
                return packageForAssetType.getName();
            }
        };
    }

    @Override
    @SneakyThrows
    public AgentCodeRegistry.AgentPackage loadPackageForAgent(String agentType) {
        PackageMetadata packageForAgentType = getPackageForAgentType(agentType);
        if (packageForAgentType == null) {
            return null;
        }
        URLClassLoader classLoader =
                createClassloaderForPackage(customCodeClassloader, packageForAgentType);
        log.info(
                "For package {}, classloader {}, parent {}",
                packageForAgentType.getName(),
                classLoader,
                classLoader.getParent());
        return new AgentCodeRegistry.AgentPackage() {
            @Override
            public ClassLoader getClassloader() {
                return classLoader;
            }

            @Override
            public String getName() {
                return packageForAgentType.getName();
            }
        };
    }

    public PackageMetadata getPackageForAgentType(String name) {
        return packages.values().stream()
                .filter(p -> p.agentTypes != null && p.agentTypes.contains(name))
                .findFirst()
                .orElse(null);
    }

    public PackageMetadata getPackageForAssetType(String name) {
        return packages.values().stream()
                .filter(p -> p.assetTypes != null && p.assetTypes.contains(name))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<? extends ClassLoader> getAllClassloaders() throws Exception {
        if (classloaders != null) {
            return classloaders;
        }

        classloaders = new ArrayList<>();
        for (PackageMetadata metadata : packages.values()) {
            metadata.unpack();
            URLClassLoader result = createClassloaderForPackage(customCodeClassloader, metadata);
            classloaders.add(result);
        }
        return classloaders;
    }

    @Override
    public ClassLoader getCustomCodeClassloader() {
        return customCodeClassloader;
    }

    private static URLClassLoader createClassloaderForPackage(
            ClassLoader parent, PackageMetadata metadata) throws Exception {

        if (metadata.classLoader != null) {
            return metadata.classLoader;
        }

        metadata.unpack();

        log.info("Creating classloader for package {}", metadata.name);
        List<URL> urls = new ArrayList<>();

        log.info("Adding agents code {}", metadata.directory);
        urls.add(metadata.directory.toFile().toURI().toURL());

        Path metaInfDirectory = metadata.directory.resolve("META-INF");
        if (Files.isDirectory(metaInfDirectory)) {

            Path dependencies = metaInfDirectory.resolve("bundled-dependencies");
            if (Files.isDirectory(dependencies)) {
                try (DirectoryStream<Path> allFiles = Files.newDirectoryStream(dependencies)) {
                    for (Path file : allFiles) {
                        if (file.getFileName().toString().endsWith(".jar")) {
                            urls.add(file.toUri().toURL());
                        }
                    }
                }
            }
        }

        URLClassLoader result = new URLClassLoader(urls.toArray(URL[]::new), parent);
        metadata.classLoader = result;
        return result;
    }
}
