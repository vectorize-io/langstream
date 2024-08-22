package ai.langstream.agents.ms365;

import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.File;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.kiota.ApiException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientUtil {

    public interface DriveItemCollector {
        void collect(String driveId, DriveItem item, String digest);
    }

    @SneakyThrows
    public static void collectDriveItems(
            GraphServiceClient client,
            final String driveId,
            DriveItem parentItem,
            Set<String> includeMimeTypes,
            Set<String> excludeMimeTypes,
            DriveItemCollector collector) {
        List<DriveItem> children =
                client.drives()
                        .byDriveId(driveId)
                        .items()
                        .byDriveItemId(parentItem.getId())
                        .children()
                        .get(
                                request -> {
                                    request.queryParameters.select =
                                            new String[] {
                                                "id", "name", "size", "cTag", "eTag", "file",
                                                "folder"
                                            };
                                    request.queryParameters.orderby = new String[] {"name asc"};
                                })
                        .getValue();
        if (children != null) {
            for (DriveItem item : children) {
                Objects.requireNonNull(item.getId());
                if (log.isDebugEnabled()) {
                    log.debug(
                            "File {} ({}), size {}, ctag {}, parents {}, last modified time {}",
                            item.getName(),
                            item.getId(),
                            item.getSize(),
                            item.getCTag());
                }
                if (item.getFolder() != null) {
                    log.debug("Folder {} ({})", item.getName(), item.getId());
                    collectDriveItems(
                            client, driveId, item, includeMimeTypes, excludeMimeTypes, collector);
                } else {
                    log.debug("File {} ({})", item.getName(), item.getId());
                    File file = item.getFile();
                    Objects.requireNonNull(file);
                    if (!includeMimeTypes.isEmpty()
                            && !includeMimeTypes.contains(file.getMimeType())) {
                        log.debug(
                                "Skipping file {} ({}) due to mime type {}",
                                item.getName(),
                                item.getId(),
                                file.getMimeType());
                        continue;
                    }
                    if (!excludeMimeTypes.isEmpty()
                            && excludeMimeTypes.contains(file.getMimeType())) {
                        log.debug(
                                "Skipping file {} ({}) due to excluded mime type {}",
                                item.getName(),
                                item.getId(),
                                file.getMimeType());
                        continue;
                    }
                    final String digest;
                    if (item.getCTag() != null) {
                        digest = item.getCTag();
                    } else if (item.getETag() != null) {
                        log.warn(
                                "Using file eTag as digest for {}, this might be end up in duplicated processing",
                                item.getId());
                        digest = item.getETag();
                    } else {
                        log.error("Not able to compute a digest for {}, skipping", item.getId());
                        continue;
                    }
                    collector.collect(driveId, item, digest);
                }
            }
        }
    }

    public static byte[] downloadDriveItem(
            GraphServiceClient client, String driveId, DriveItem item) throws Exception {
        Objects.requireNonNull(item.getFile());
        String mimeType = item.getFile().getMimeType();
        log.info(
                "Downloading file {} ({}) from drive {}, mime type {}",
                item.getName(),
                item.getId(),
                driveId,
                mimeType);
        try {
            return downloadDriveItem(client, driveId, item, true);
        } catch (ApiException e) {
            log.info(
                    "Downloading file {} ({}) from drive {} as pdf failed, trying with default format. error: {}",
                    item.getName(),
                    item.getId(),
                    driveId,
                    e.getMessage());
            try {
                return downloadDriveItem(client, driveId, item, false);
            } catch (ApiException ex) {
                log.error(
                        "Error downloading file " + item.getName() + " (" + item.getId() + ")", e);
            }
            throw e;
        }
    }

    private static byte[] downloadDriveItem(
            GraphServiceClient client, String driveId, DriveItem item, boolean exportAsPdf)
            throws IOException {

        try (InputStream in =
                client.drives()
                        .byDriveId(driveId)
                        .items()
                        .byDriveItemId(item.getId())
                        .content()
                        .get(
                                requestConfiguration -> {
                                    Objects.requireNonNull(requestConfiguration.queryParameters);
                                    if (exportAsPdf) {
                                        requestConfiguration.queryParameters.format = "pdf";
                                    }
                                }); ) {
            if (in == null) {
                throw new IllegalStateException(
                        "No content for file " + item.getName() + " (" + item.getId() + ")");
            }
            return in.readAllBytes();
        }
    }
}
