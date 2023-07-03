package com.datastax.oss.sga.cli.commands.applications;

import com.datastax.oss.sga.cli.SgaCLIConfig;
import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.datastax.oss.sga.cli.commands.RootAppCmd;
import com.datastax.oss.sga.cli.commands.RootCmd;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import lombok.SneakyThrows;
import picocli.CommandLine;

public abstract class BaseApplicationCmd extends BaseCmd {

    @CommandLine.ParentCommand
    private RootAppCmd rootAppCmd;


    @Override
    protected RootCmd getRootCmd() {
        return rootAppCmd.getRootCmd();
    }
}
