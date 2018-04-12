Deriggy
-------

A simple prototype library, built on [Apache Fluo][fluo], that maintains a
derived graph created from multiple raw graphs.  As the raw graphs change, the
derived graph is updated.  For an example of this library in action, see the
[Mixer] project.

The API entry point for this library is [Deriggy][Deriggy.java].

To depend on this project via Maven, add the following to your pom :

```xml
<dependency>
    <groupId>com.github.keith-turner</groupId>
    <artifactId>deriggy</artifactId>
    <version>1.0.0</version>
</dependency>
```

[Deriggy.java]: src/main/java/deriggy/api/Deriggy.java
[fluo]: https://fluo.apache.org
[Mixer]: https://github.com/keith-turner/mixer

