# Hive4Net

Hive4Net is a library to run hive queries with Apache Thrift

Examples :
------
.. code-block:: c#

    Hive hive = new Hive("localhost", 10001, "admin", "myPassword");
	await hive.OpenAsync();
	var cursor = hive.GetCursorAsync();
	cursor.ExecuteAsync("SHOW TABLES");
	var result = cursor.FetchAsync(100);
