abstract class Processor {
  def run(row: String): String
}

object Planet extends Enumeration {
  protected case class Val(name: String, processor: Processor) extends super.Val
  import scala.language.implicitConversions
  implicit def valueToPlanetVal(x: Value): Val = x.asInstanceOf[Val]

  def withNameWithDefault(name: String): Option[Value] = {
    Planet.values.find(_.toString.toLowerCase == name.toLowerCase)
  }

  val Mercury = Val("mercury", new Processor {
    def run(row: String): String = {
      "in merc"
    }
  })
  val Venus   = Val("venus", new Processor {
    override def run(row: String): String = {
      "in venus"
    }
  })
}

class Record(row: String) {
  def get(field: Planet.Value): String = {
    field.toString
  }
}

println(Planet.Mercury.processor.run("test"))
println(Planet.withNameWithDefault("mercury").get.processor.run("Test"))
println(new Record("blaah").get(Planet.Mercury))