package com.weez.mercury.imports

import scala.annotation.StaticAnnotation

class packable extends StaticAnnotation {
  import scala.language.experimental.macros

  def macroTransform(annottees: Any*): Any = macro com.weez.mercury.macros.PackerMacro.packableImpl
}
