## Java Examples for Stream Processing with Apache Flink

### 原始项目内容
This repository hosts Java code examples for ["Stream Processing with Apache Flink"](http://shop.oreilly.com/product/0636920057321.do) by [Fabian Hueske](https://twitter.com/fhueske) and [Vasia Kalavri](https://twitter.com/vkalavri).

The [Scala examples](https://github.com/streaming-with-flink/examples-scala) are complete and we are working on translating them to Java.

<a href="http://shop.oreilly.com/product/0636920057321.do">
  <img width="240" src="https://covers.oreillystatic.com/images/0636920057321/cat.gif">
</a>

### 本项目说明
该项目为原始examples-java的项目的clone，由@author Yitian实现。<br>
1. master分支: 原始examples-java的初始化项目。<br>
2. dev分支（当前默认分支）: 在原始项目基础上完善Java版本代码内容，代码主要根据Scala版本代码使用Java进行重新实现。

### maven项目打包命令
```
mvn clean package -Pbuild-jar
```
