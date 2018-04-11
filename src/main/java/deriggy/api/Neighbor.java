package deriggy.api;

import java.util.Objects;

public class Neighbor {

  public final String id;
  public final String type;

  public Neighbor(String id, String type) {
    this.id = Objects.requireNonNull(id);
    this.type = Objects.requireNonNull(type);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id.hashCode();
    result = prime * result + type.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof Neighbor))
      return false;

    Neighbor other = (Neighbor) obj;

    return id.equals(other.id) && type.equals(other.type);
  }

  @Override
  public String toString() {
    return id + ":" + type;
  }
}
