package com.weez.mercury.common

import com.typesafe.config.Config

class ResourceManager(config: Config) {
  private val baseDir = FileIO.pathOfExp(config.getString("resource-directory"))

}
