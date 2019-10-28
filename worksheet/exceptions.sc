val fn = (x: Int) => {
  try {
    if (x > 3) throw new RuntimeException
    1
  } catch {
    case _: IllegalArgumentException => -1;
  }
}

fn(3)

fn(4)
