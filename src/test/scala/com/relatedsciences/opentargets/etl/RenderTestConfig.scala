package com.relatedsciences.opentargets.etl
import com.typesafe.config.{ConfigRenderOptions, ConfigValue}

/**
* Utility class used to render test configuration from HOCON parts
  */
object RenderTestConfig {

  def main(args: Array[String]): Unit = {
    println(TestUtils.getConfigSource("/config/application.conf")
      .loadOrThrow[ConfigValue].render(ConfigRenderOptions.concise().setFormatted(true).setJson(false)))
  }
}
