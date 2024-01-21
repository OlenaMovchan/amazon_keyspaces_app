package com.shpp;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class KeyspacesConfigLoader {

    public Config loadConfig() {
        return ConfigFactory.load("application.conf");
    }
}
