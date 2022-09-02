Astraea Dispatcher 中文文件
===
使用Astraea Dispatcher替换掉Kafka Partitioner module，將發送的每筆資料按照節點負載狀況進行合理分配，達到發送端的負載平衡。

### 通過gradle引入Astraea
在build.gradle中添加以下內容
```gradle
repositories {
    maven {
        url = "https://maven.pkg.github.com/skiptests/astraea"
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
    
    dependencies {
        implementation 'org.astraea:app:0.0.1-SNAPSHOT'
    }
}
```

### Astraea Dispatcher實做
1. [Smooth Dispatcher](smooth_dispatcher.md):  通過多metrics評估節點狀況，進行Producer端發送資料的調配。