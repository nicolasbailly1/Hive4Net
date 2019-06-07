# Hive4Net

Hive4Net is a library to run hive queries with Apache Thrift

# Dependency

ApacheThrift : https://www.nuget.org/packages/ApacheThrift/

## Examples
```c#
    Hive hive = new Hive("localhost", 10001, "admin", "myPassword");
    await hive.OpenAsync();
    var cursor = hive.GetCursorAsync();
    cursor.ExecuteAsync("SHOW TABLES");
    var result = cursor.FetchAsync(100);
```
