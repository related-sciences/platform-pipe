import scala.util.parsing.json.{JSONObject, JSONFormat, JSONArray, JSON}

def formatJSON(t: Any, i: Int = 0): String = t match {
  case o: JSONObject =>
    o.obj
      .map {
        case (k, v) =>
          "  " * (i + 1) + JSONFormat.defaultFormatter(k) + ": " + formatJSON(v, i + 1)
      }
      .mkString("{\n", ",\n", "\n" + "  " * i + "}")

  case a: JSONArray =>
    a.list
      .map { e =>
        "  " * (i + 1) + formatJSON(e, i + 1)
      }
      .mkString("[\n", ",\n", "\n" + "  " * i + "]")

  case _ => JSONFormat defaultFormatter t
}

def printJSON(o: String) = {
  println(formatJSON(JSON.parseRaw(o).get, i = 1))
}
