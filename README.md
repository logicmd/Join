Join via MapReduce
==================
在Web 2.0时代大规模数据处理非常重要，本项目只在提供一个利用Hadoop实现的Join操作（数据库中最重要的操作）。


Structure
=========

论文在doc中，实验结果在exp中，源代码放在src中。

    ─edu
      └─pku
          ├─broadcast
          │      BroadcastJoin.java
          │      BroadcastMapper.java
          │
          ├─mapside
          │      MapSideJoin.java
          │      SequenceFileIO.java
          │      Sort.java
          │
          ├─reduceside
          │      JoinReducer.java
          │      ReduceSideJoin.java
          │      TableOneMapper.java
          │      TableTwoMapper.java
          │
          ├─reducesidenew
          │      ReduceSideJoinNew.java
          │
          ├─test
          │      ConcatTest.java
          │      DatasetGen.java
          │      JobConfTest.java
          │
          └─util
                  DatasetFactory.java
                  TableOneParser.java
                  TableTwoParser.java
                  TextPair.java

所有源码放在edu.pku.*的Package下面。

- edu.pku.util是整个Join框架所需的工具集，包括两个Table的Parser用于封装parse过程，让map阶段显得更干净。DatasetFactory用于生产测试数据。TextPair是一个自定义的数据类型，用于生产text对。
- ```edu.pku.mapside``` 是Mapside Join的实现，包括Mapper和Join过程。
- ```edu.pku.reduceside``` 是Reduceside Join的实现，包括Mapper，Reducer和Join过程。
- ```edu.pku.reducesidenew``` 是Reduceside Join NEW API的实现，包括Mapper，Reducer和Join过程。
- ```edu.pku.broadcast``` 是BroadcastJoin的实现，包括Mapper和Join过程。
- ```edu.pku.test``` 是少量中间test。
